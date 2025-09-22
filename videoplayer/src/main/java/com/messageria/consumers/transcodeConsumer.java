package com.messageria.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messageria.config.rabbitMQConfig;
import com.messageria.repository.FileJobRepository;
import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TranscodeConsumer {

    private static final String QUEUE = "transcode.queue";
    private static final String EXCHANGE = "video.exchange";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        rabbitMQConfig cfg = new rabbitMQConfig();

        // repository para idempotencia
        FileJobRepository repo = new FileJobRepository(Path.of("state"));

        try (Connection conn = cfg.createConnection();
             Channel ch = conn.createChannel()) {

            System.out.println("[*] Transcode Consumer aguardando mensagens...");

            // prefetch 1 (CPU-bound)
            ch.basicQos(1);

            Consumer consumer = new DefaultConsumer(ch) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String payload = new String(body, StandardCharsets.UTF_8);
                    String jobId = null;
                    String videoId = null;
                    String inputUrl = null;

                    try {
                        JsonNode root = MAPPER.readTree(payload);
                        // esperado: { "jobId":"job-1", "videoId":"media-123", "inputUrl":"uploads/media-123.mp4" }
                        jobId = root.path("jobId").asText(null);
                        videoId = root.path("videoId").asText(null);
                        inputUrl = root.path("inputUrl").asText(null);

                        if (jobId == null || videoId == null || inputUrl == null) {
                            throw new IllegalArgumentException("Mensagem inválida, faltando jobId/videoId/inputUrl: " + payload);
                        }

                        // idempotência
                        if (repo.isFinished(jobId)) {
                            System.out.println("[i] Job já processado: " + jobId + " -> ack");
                            ch.basicAck(deliveryTag, false);
                            return;
                        }

                        // localizar input file (suporta path absoluto ou relativo)
                        File inputFile = new File(inputUrl);
                        if (!inputFile.exists()) {
                            System.err.println("[!] Arquivo de input não encontrado: " + inputFile.getAbsolutePath());
                            // erro permanente -> nack sem requeue -> DLQ
                            ch.basicNack(deliveryTag, false, false);
                            return;
                        }

                        // preparar outputs
                        File outDir = new File("outputs", videoId);
                        if (!outDir.exists()) outDir.mkdirs();
                        File out720 = new File(outDir, videoId + "_720p.mp4");
                        File out480 = new File(outDir, videoId + "_480p.mp4");

                        // construir comandos ffmpeg
                        List<String> cmd720 = List.of("C:\\ffmpeg\\bin\\ffmpeg.exe", "-y", "-i", inputFile.getAbsolutePath(),
                                "-vf", "scale=-2:720", "-c:v", "libx264", "-preset", "fast", "-b:v", "2500k",
                                "-c:a", "aac", "-b:a", "128k", out720.getAbsolutePath());

                        List<String> cmd480 = List.of("C:\\ffmpeg\\bin\\ffmpeg.exe", "-y", "-i", inputFile.getAbsolutePath(),
                                "-vf", "scale=-2:480", "-c:v", "libx264", "-preset", "fast", "-b:v", "1000k",
                                "-c:a", "aac", "-b:a", "96k", out480.getAbsolutePath());

                        // executa transcode (720p)
                        System.out.println("[*] Executando ffmpeg 720p para job=" + jobId);
                        runProcess(cmd720, 20, TimeUnit.MINUTES);

                        System.out.println("[*] Executando ffmpeg 480p para job=" + jobId);
                        runProcess(cmd480, 15, TimeUnit.MINUTES);

                        // opcional: aqui você faria upload para S3/MinIO e obteria URLs finais
                        // para simplicidade em dev, usamos os caminhos locais
                        List<String> outputs = new ArrayList<>();
                        outputs.add(out720.getAbsolutePath());
                        outputs.add(out480.getAbsolutePath());

                        // marcar job como finalizado (idempotência)
                        repo.markFinished(jobId);

                        // publicar evento de transcode finished
                        String finishedJson = MAPPER.createObjectNode()
                                .put("event", "TranscodeJobFinished")
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

                        ch.basicPublish(EXCHANGE, "transcode.job.finished", props, finishedJson.getBytes(StandardCharsets.UTF_8));
                        System.out.println("[✓] Job " + jobId + " finalizado. outputs: " + outputs);

                        // ack final
                        ch.basicAck(deliveryTag, false);
                    } catch (Exception ex) {
                        System.err.println("[!] Erro processando transcode jobId=" + jobId + " videoId=" + videoId + " -> " + ex.getMessage());
                        ex.printStackTrace();
                        // regra simples: nack sem requeue -> vai pro DLQ (retry via TTL não implementado aqui)
                        try {
                            ch.basicNack(deliveryTag, false, false);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };

            ch.basicConsume(QUEUE, false, consumer);
            // manter app vivo
            System.in.read();
        }
    }

    private static void runProcess(List<String> cmd, long timeout, TimeUnit unit) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        // consume output stream to avoid blocking
        Thread t = new Thread(() -> {
            try (InputStream is = p.getInputStream();
                 BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // opcional: log
                    // System.out.println("[ffmpeg] " + line);
                }
            } catch (IOException ignored) {}
        });
        t.start();

        boolean finished = p.waitFor(timeout, unit);
        if (!finished) {
            p.destroyForcibly();
            throw new RuntimeException("ffmpeg timed out (killed)");
        }
        if (p.exitValue() != 0) {
            throw new RuntimeException("ffmpeg failed with exit code " + p.exitValue());
        }
    }
}
