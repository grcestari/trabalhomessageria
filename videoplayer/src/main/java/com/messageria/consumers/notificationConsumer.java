package com.messageria.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messageria.config.rabbitMQConfig;
import com.rabbitmq.client.*;

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
 * - Quando transcode+thumbnail estiverem prontos envia notificação final (HTTP
 * POST se NOTIF_ENDPOINT set)
 * - Marca como notificado e evita duplicações (idempotência)
 *
 * Observação: garanta que ThumbnailConsumer publique "thumbnail.created" no
 * exchange após gerar thumbnail.
 */
public class NotificationConsumer {
    private static final String QUEUE = "notificacao.queue";
    private static final String EXCHANGE = "video.exchange";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // pasta de estado onde guardamos JSON por videoId
    private static final Path STATE_DIR = Paths.get("state", "notifications");

    // endpoint para enviar notificações
    private static final String NOTIF_ENDPOINT = System.getenv().getOrDefault("NOTIF_ENDPOINT", "").trim();

    public static void main(String[] args) throws Exception {
        if (!Files.exists(STATE_DIR))
            Files.createDirectories(STATE_DIR);

        System.out.println("Iniciando Notificacao Consumer");

        rabbitMQConfig cfg = new rabbitMQConfig();
        try (Connection conn = cfg.createConnection();
                Channel ch = conn.createChannel()) {

            System.out.println("Notificacao Consumer aguardando mensagens");

            ch.basicQos(5);

            DeliverCallback callback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);

                String videoId = null;
                String jobId = null;
                String inputUrl = null;
                try {
                    JsonNode root = MAPPER.readTree(raw);
                    jobId = root.path("jobId").asText(null);
                    videoId = root.path("videoId").asText(null);
                    inputUrl = root.path("inputUrl").asText(null);
                    String event = root.path("event").asText(null);

                    if (jobId == null || videoId == null || inputUrl == null) {
                        throw new IllegalArgumentException(
                                "Mensagem inválida, faltando jobId/videoId/inputUrl: " + raw);
                    }

                    System.out.println("Evento recebido: event=" + event + " videoId=" + videoId + " jobId=" + jobId);

                    // Carregar state atual
                    NotificationState state = loadState(videoId);
                    boolean stateChanged = false;

                    if ("TranscodeCreated".equals(event) || "transcode.created".equals(root.path("event").asText())) {
                        state.transcodeDone = true;

                        if (root.has("outputs")) {
                            state.outputs = root.path("outputs").toString();
                        }
                        stateChanged = true;
                    } else if ("ThumbnailCreated".equals(event)  || "thumbnail.created".equals(root.path("event").asText())) {
                        state.thumbnailDone = true;
                        if (root.has("thumbnail")) {
                            state.thumbnailPath = root.path("thumbnail").asText(null);
                        }
                        stateChanged = true;
                    }

                    if (stateChanged)
                        saveState(videoId, state);

                    // Se ambos prontos e ainda não notificado -> notificar
                    if (state.transcodeDone && state.thumbnailDone && !state.notified) {
                        System.out.println("Enviando notificação");

                        Map<String, Object> payload = new HashMap<>();
                        payload.put("videoId", videoId);
                        payload.put("event", "VideoReady");
                        payload.put("outputs",
                                state.outputs != null ? MAPPER.readTree(state.outputs) : MAPPER.createArrayNode());
                        if (state.thumbnailPath != null)
                            payload.put("thumbnail", state.thumbnailPath);

                        String payloadJson = MAPPER.writeValueAsString(payload);

                        boolean sent = sendNotification(payloadJson);
                        if (sent) {
                            state.notified = true;
                            state.notifiedAt = Instant.now().toString();
                            saveState(videoId, state);
                            System.out.println("[✓] Notificação enviada para videoId={" + videoId + "}");
                            ch.basicAck(deliveryTag, false);
                        } else {
                            System.out.println(
                                    "[!] Falha ao enviar notificação para videoId={" + videoId + "} -> nack (DLQ)");
                            ch.basicNack(deliveryTag, false, false);
                        }
                    } else {
                        // não pronto ainda: ack só essa mensagem (guardamos estado)
                        // LOGGER.info("[i] Estado atualizado para videoId={} (transcode={},
                        // thumbnail={}, notified={})",
                        // videoId, state.transcodeDone, state.thumbnailDone, state.notified);
                        System.out.println("[i] Estado atualizado para videoId={" + videoId);
                        ch.basicAck(deliveryTag, false);
                    }

                } catch (Exception ex) {
                    System.out.println("[!] Erro processando notificação (raw=" + raw + "): " + ex.getMessage());
                    try {
                        ch.basicNack(deliveryTag, false, false);
                    } catch (IOException ioe) {
                        System.out.println("Erro ao nack: {}" + ioe.getMessage());
                    }
                }
            };

            ch.basicConsume(QUEUE, false, callback, consumerTag -> {
            });

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
            Path p = STATE_DIR.resolve(videoId + ".json");
            if (Files.exists(p)) {
                byte[] b = Files.readAllBytes(p);
                return MAPPER.readValue(b, NotificationState.class);
            }
        } catch (Exception e) {
            System.out.println("Erro carregando state");
        }
        return new NotificationState();
    }

    private static void saveState(String videoId, NotificationState s) {
        try {
            Path p = STATE_DIR.resolve(videoId + ".json");
            byte[] b = MAPPER.writeValueAsBytes(s);
            Files.write(p, b, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            System.out.println("Erro salvando state");
        }
    }

    // private static Path stateFileFor(String videoId) {
    //     // sanitize filename similar ao FileJobRepository
    //     String safe = videoId.replaceAll("[\\\\/:\\*?\"<>|]", "_").replaceAll("\\s+", "_");
    //     if (safe.length() > 200)
    //         safe = safe.substring(0, 200);
    //     return STATE_DIR.resolve(safe + ".json");
    // }

    private static boolean sendNotification(String jsonPayload) {
        if (NOTIF_ENDPOINT.isEmpty()) {
            System.out.println("[notify][noop] payload=" + jsonPayload);
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
            if (sc >= 200 && sc < 300)
                return true;
            System.out.println("[!] Webhook retornou status=" + sc);
            return false;
        } catch (Exception e) {
            System.out.println("[!] Falha na requisição HTTP: {}" + e.getMessage());
            return false;
        } finally {
            if (conn != null)
                conn.disconnect();
        }
    }

    private static String readStream(InputStream is) {
        if (is == null)
            return "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null)
                sb.append(line).append('\n');
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
