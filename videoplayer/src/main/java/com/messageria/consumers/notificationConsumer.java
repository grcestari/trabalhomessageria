package com.messageria.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messageria.config.rabbitMQConfig;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * NotificationAggregatorConsumer
 *
 * - Aguarda eventos de transcode.job.finished e thumbnail.created
 * - Mantém estado por videoId em state/notifications/<videoId>.json
 * - Quando transcode+thumbnail estiverem prontos envia notificação final (HTTP POST se NOTIF_ENDPOINT set)
 * - Marca como notificado e evita duplicações (idempotência)
 *
 * Observação: garanta que ThumbnailConsumer publique "thumbnail.created" no exchange após gerar thumbnail.
 */
public class NotificationConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationConsumer.class);
    private static final String QUEUE = "notificacao.queue"; // já existente no seu rabbitMQConfig
    private static final String EXCHANGE = "video.exchange";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // pasta de estado onde guardamos JSON por videoId
    private static final Path STATE_DIR = Paths.get("state", "notifications");

    // endpoint para enviar notificações (opcional)
    private static final String NOTIF_ENDPOINT = System.getenv().getOrDefault("NOTIF_ENDPOINT", "").trim();

    public static void main(String[] args) throws Exception {
        // garantir pasta de state
        if (!Files.exists(STATE_DIR)) Files.createDirectories(STATE_DIR);

        rabbitMQConfig cfg = new rabbitMQConfig();
        try (Connection conn = cfg.createConnection();
             Channel ch = conn.createChannel()) {

            LOGGER.info("[*] Notification Aggregator aguardando mensagens... (NOTIF_ENDPOINT={})", NOTIF_ENDPOINT);

            // prefetch razoável
            ch.basicQos(5);

            DeliverCallback callback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
                try {
                    JsonNode root = MAPPER.readTree(raw);
                    String event = root.path("event").asText(null);

                    // tentar extrair videoId de vários formatos (compatibilidade)
                    String videoId = root.path("videoId").asText(null);
                    if ((videoId == null || videoId.isEmpty()) && root.has("mediaId")) {
                        videoId = root.path("mediaId").asText(null);
                    }
                    if (videoId == null || videoId.isEmpty()) {
                        LOGGER.warn("[!] Mensagem sem videoId ignorada (raw={}): ack", raw);
                        ch.basicAck(deliveryTag, false);
                        return;
                    }

                    LOGGER.info("[x] 'Evento' recebido: event={} videoId={}", event, videoId);

                    // Carregar state atual
                    NotificationState state = loadState(videoId);

                    boolean stateChanged = false;

                    if ("TranscodeJobFinished".equals(event) || "transcode.job.finished".equals(root.path("event").asText())) {
                        state.transcodeDone = true;
                        // captura outputs se houver
                        if (root.has("outputs")) {
                            state.outputs = root.path("outputs").toString();
                        }
                        stateChanged = true;
                    } else if ("ThumbnailCreated".equals(event) || "thumbnail.created".equals(root.path("event").asText())) {
                        state.thumbnailDone = true;
                        if (root.has("thumbnail")) {
                            state.thumbnailPath = root.path("thumbnail").asText(null);
                        }
                        stateChanged = true;
                    } else if (root.has("videoId") && root.has("status")) {
                        // mensagens legacy (upload) — opcional: apenas registrar
                        state.lastSeenStatus = root.path("status").asText();
                        stateChanged = true;
                    } else {
                        LOGGER.warn("[?] Evento desconhecido: {} - ack", raw);
                        ch.basicAck(deliveryTag, false);
                        return;
                    }

                    if (stateChanged) saveState(videoId, state);

                    // Se ambos prontos e ainda não notificado -> notificar
                    if (state.transcodeDone && state.thumbnailDone && !state.notified) {
                        LOGGER.info("[*] videoId={} pronto (transcode+thumbnail). Enviando notificação...", videoId);
                        // construir payload simples; você pode customizar
                        Map<String,Object> payload = new HashMap<>();
                        payload.put("videoId", videoId);
                        payload.put("event", "VideoReady");
                        payload.put("outputs", state.outputs != null ? MAPPER.readTree(state.outputs) : MAPPER.createArrayNode());
                        if (state.thumbnailPath != null) payload.put("thumbnail", state.thumbnailPath);

                        String payloadJson = MAPPER.writeValueAsString(payload);

                        boolean sent = sendNotification(payloadJson);
                        if (sent) {
                            state.notified = true;
                            state.notifiedAt = Instant.now().toString();
                            saveState(videoId, state);
                            LOGGER.info("[✓] Notificação enviada para videoId={}", videoId);
                            ch.basicAck(deliveryTag, false);
                        } else {
                            LOGGER.error("[!] Falha ao enviar notificação para videoId={} -> nack (DLQ)", videoId);
                            ch.basicNack(deliveryTag, false, false);
                        }
                    } else {
                        // não pronto ainda: ack só essa mensagem (guardamos estado)
                        LOGGER.info("[i] Estado atualizado para videoId={} (transcode={}, thumbnail={}, notified={})",
                                videoId, state.transcodeDone, state.thumbnailDone, state.notified);
                        ch.basicAck(deliveryTag, false);
                    }

                } catch (Exception ex) {
                    LOGGER.error("[!] Erro processando notificação (raw={}): {}", raw, ex.getMessage(), ex);
                    try { ch.basicNack(deliveryTag, false, false); } catch (IOException ioe) { LOGGER.error("Erro ao nack: {}", ioe.getMessage()); }
                }
            };

            ch.basicConsume(QUEUE, false, callback, consumerTag -> { });

            // manter app vivo
            System.in.read();
        }
    }

    // simples estado persistido por ficheiro JSON
    static class NotificationState {
        public boolean transcodeDone = false;
        public boolean thumbnailDone = false;
        public boolean notified = false;
        public String outputs = null; // JSON array string
        public String thumbnailPath = null;
        public String lastSeenStatus = null;
        public String notifiedAt = null;
    }

    private static NotificationState loadState(String videoId) {
        try {
            Path p = stateFileFor(videoId);
            if (Files.exists(p)) {
                byte[] b = Files.readAllBytes(p);
                return MAPPER.readValue(b, NotificationState.class);
            }
        } catch (Exception e) {
            LOGGER.warn("Erro carregando state para {}: {}", videoId, e.getMessage());
        }
        return new NotificationState();
    }

    private static void saveState(String videoId, NotificationState s) {
        try {
            Path p = stateFileFor(videoId);
            byte[] b = MAPPER.writeValueAsBytes(s);
            Files.write(p, b, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            LOGGER.error("Erro salvando state para {}: {}", videoId, e.getMessage(), e);
        }
    }

    private static Path stateFileFor(String videoId) {
        // sanitize filename similar ao FileJobRepository
        String safe = videoId.replaceAll("[\\\\/:\\*?\"<>|]", "_").replaceAll("\\s+", "_");
        if (safe.length() > 200) safe = safe.substring(0, 200);
        return STATE_DIR.resolve(safe + ".json");
    }

    private static boolean sendNotification(String jsonPayload) {
        if (NOTIF_ENDPOINT.isEmpty()) {
            LOGGER.info("[notify][noop] payload={}", jsonPayload);
            return true; // consider as "sent" in dev mode
        }
        HttpURLConnection conn = null;
        try {
            URL url = new URL(NOTIF_ENDPOINT);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(20_000);
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }
            int sc = conn.getResponseCode();
            if (sc >= 200 && sc < 300) return true;
            LOGGER.error("Webhook retornou status={} body={}", sc, readStream(conn.getErrorStream()));
            return false;
        } catch (Exception e) {
            LOGGER.error("Falha na requisição HTTP: {}", e.getMessage());
            return false;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private static String readStream(InputStream is) {
        if (is == null) return "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line).append('\n');
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
