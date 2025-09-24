package com.messageria.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messageria.config.rabbitMQConfig;
import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class NotificationConsumer {
    private static final String QUEUE = "notificacao.queue";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Path STATE_DIR = Paths.get("state", "notifications");

    public static void main(String[] args) throws Exception {
        if (!Files.exists(STATE_DIR))
            Files.createDirectories(STATE_DIR);

        System.out.println("Iniciando Notificacao Consumer");

        rabbitMQConfig cfg = new rabbitMQConfig();
        try (Connection conn = cfg.createConnection();
                Channel channel = conn.createChannel()) {

            System.out.println("Notificacao Consumer aguardando mensagens");

            channel.basicQos(5);

            DeliverCallback callback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);

                String videoId = null;
                String event = null;

                try {
                    JsonNode root = MAPPER.readTree(raw);
                    videoId = root.path("videoId").asText(null);
                    event = root.path("event").asText(null);

                    if (videoId == null) {
                        throw new IllegalArgumentException(
                                "Mensagem inválida, faltando jobId/videoId/inputUrl: " + raw);
                    }

                    NotificationState state = loadState(videoId);
                    boolean stateChanged = false;

                    if ("transcodecreated".equalsIgnoreCase(event)) {
                        state.transcodeDone = true;
                        if (root.has("outputs")) {
                            state.outputs = root.path("outputs").toString();
                        }
                        stateChanged = true;
                    } else if ("thumbnailcreated".equalsIgnoreCase(event)) {
                        state.thumbnailDone = true;
                        if (root.has("thumbnail")) {
                            state.thumbnailPath = root.path("thumbnail").asText(null);
                        }
                        stateChanged = true;
                    }

                    System.out.println("Estado atual do videoId=" + videoId + ": transcodeDone=" 
                   + state.transcodeDone + ", thumbnailDone=" + state.thumbnailDone);

                    if (stateChanged)
                        saveState(videoId, state);

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

                        System.out.println("Notificação enviada: " + payloadJson);

                        state.notified = true;
                        state.notifiedAt = Instant.now().toString();
                        saveState(videoId, state);

                        channel.basicAck(deliveryTag, false);
                    } else {
                        System.out.println("Algum dos jobs ainda não está pronto, aguardando mais eventos");
                        channel.basicAck(deliveryTag, false);
                    }

                } catch (Exception ex) {
                    System.out.println("erro processando notificação" + ex.getMessage());
                    try {
                        channel.basicNack(deliveryTag, false, false);
                    } catch (IOException ioe) {
                        System.out.println("Erro ao enviar nack: " + ioe.getMessage());
                    }
                }
            };

            channel.basicConsume(QUEUE, false, callback, consumerTag -> {
            });

            System.in.read();
        }
    }

    static class NotificationState {
        public boolean transcodeDone = false;
        public boolean thumbnailDone = false;
        public boolean notified = false;
        public String outputs = null;
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
}
