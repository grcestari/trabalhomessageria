package com.messageria.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messageria.config.rabbitMQConfig;
import com.messageria.repository.FileJobRepository;
import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TranscodeConsumer {

    private static final String QUEUE = "transcode.queue";
    private static final String EXCHANGE = "video.exchange";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando Transcode Consumer");
        rabbitMQConfig cfg = new rabbitMQConfig();
        FileJobRepository repo = new FileJobRepository(Path.of("state"));

        try (Connection conn = cfg.createConnection(); Channel channel = conn.createChannel()) {

            System.out.println("Transcode Consumer aguardando mensagens");
            channel.basicQos(1);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                String videoId = null;
                String jobId = "thumb-" + videoId + "-" + Instant.now().toEpochMilli();
                String inputUrl = null;

                try {
                    JsonNode root = MAPPER.readTree(raw);
                    videoId = root.path("videoId").asText(null);
                    inputUrl = root.path("inputUrl").asText(null);

                    if ( videoId == null || inputUrl == null) {
                        throw new IllegalArgumentException(
                                "Mensagem inválida, faltando jobId/videoId/inputUrl: " + raw);
                    }

                    jobId = "transcode-" + videoId + "-" + Instant.now().toEpochMilli();

                    System.out.println("Mensagem recebida: videoId='" + videoId + "' jobId='" + jobId + "'");

                    if (repo.isFinished(jobId)) {
                        System.out.println("Job já processado (jobId=" + jobId + ")");
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    File videoFile = new File(inputUrl);
                    System.out.println("Procurando arquivo de vídeo em: " + inputUrl);

                    if (!videoFile.exists()) {
                        System.err.println(" Arquivo de input não encontrado: " + videoFile.getAbsolutePath());
                        channel.basicNack(deliveryTag, false, false);
                        return;
                    }

                    File outDir = new File("outputs", videoId + "_transcode");
                    if (!outDir.exists())
                        outDir.mkdirs();
                    File out720 = new File(outDir, videoId + "_720p.mp4");
                    File out480 = new File(outDir, videoId + "_480p.mp4");

                    List<String> cmd720 = List.of("C:\\ffmpeg\\bin\\ffmpeg.exe", "-y", "-i",
                            videoFile.getAbsolutePath(), "-vf", "scale=-2:720", "-c:v", "libx264",
                            "-preset", "fast", "-b:v", "2500k", "-c:a", "aac", "-b:a", "128k",
                            out720.getAbsolutePath());

                    List<String> cmd480 = List.of("C:\\ffmpeg\\bin\\ffmpeg.exe", "-y", "-i",
                            videoFile.getAbsolutePath(), "-vf", "scale=-2:480", "-c:v", "libx264",
                            "-preset", "fast", "-b:v", "1000k", "-c:a", "aac", "-b:a", "96k", out480.getAbsolutePath());

                    System.out.println("Executando ffmpeg 720p para job=" + jobId);
                    runProcess(cmd720, 20, TimeUnit.MINUTES);

                    System.out.println("Executando ffmpeg 480p para job=" + jobId);
                    runProcess(cmd480, 15, TimeUnit.MINUTES);

                    List<String> outputs = new ArrayList<>();
                    outputs.add(out720.getAbsolutePath());
                    outputs.add(out480.getAbsolutePath());

                    repo.markFinished(jobId);
                    
                    String transcodeJSON = MAPPER.createObjectNode()
                            .put("event", "TranscodeCreated")
                            .put("jobId", jobId)
                            .put("videoId", videoId)
                            .putPOJO("outputs", outputs)
                            .toString();

                    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                            .contentType("application/json")
                            .messageId(jobId)
                            .correlationId(videoId)
                            .deliveryMode(2)
                            .build();

                    channel.basicPublish(EXCHANGE, "transcode.created", props,
                            transcodeJSON.getBytes(StandardCharsets.UTF_8));

                    System.out.println("Publicado transcode.created,  videoId=" + videoId + " jobId=" + jobId);

                    channel.basicAck(deliveryTag, false);

                } catch (Exception e) {
                    System.out.println("Falha processando thumbnail para videoId='" + videoId + "' jobId='" + jobId
                            + "'. Enviando para DLQ. Erro: " + e.getMessage());
                    try {
                        channel.basicNack(deliveryTag, false, false);
                    } catch (IOException ioe) {
                        System.out.println("Erro ao enviar nack: " + ioe.getMessage());
                    }
                }
            };

            CancelCallback cancelCallback = consumerTag -> System.out.println("Consumer cancelado: " + consumerTag);

            channel.basicConsume(QUEUE, false, deliverCallback, cancelCallback);

            System.in.read();
        }
    }

    private static void runProcess(List<String> cmd, long timeout, TimeUnit unit) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        Thread t = new Thread(() -> {
            try (InputStream is = p.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                while (br.readLine() != null) {
                }
            } catch (IOException ignored) {
            }
        });
        t.start();

        boolean finished = p.waitFor(timeout, unit);
        if (!finished) {
            p.destroyForcibly();
            throw new RuntimeException("Time out");
        }
        if (p.exitValue() != 0) {
            throw new RuntimeException("Erro: " + p.exitValue());
        }
    }
}
