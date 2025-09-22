package com.messageria.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messageria.config.rabbitMQConfig;
import com.messageria.repository.FileJobRepository;
import com.rabbitmq.client.*;
import org.jcodec.api.FrameGrab;
import org.jcodec.common.model.Picture;
import org.jcodec.common.io.NIOUtils;
import org.jcodec.common.io.SeekableByteChannel;
import org.jcodec.containers.mp4.demuxer.MP4Demuxer;
import org.jcodec.scale.AWTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;

public class ThumbnailConsumer {
    private static final String QUEUE_NAME = "thumbnail.queue";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ThumbnailConsumer.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Iniciando Thumbnail Consumer...");
        rabbitMQConfig config = new rabbitMQConfig();

        // repo para idempotência (dev). Em produção troque por DB/Redis.
        FileJobRepository repo = new FileJobRepository(Path.of("state"));

        try (Connection connection = config.createConnection();
                Channel channel = connection.createChannel()) {

            LOGGER.info("[*] Thumbnail Consumer aguardando mensagens. Para sair: CTRL+C");

            // Prefetch control: número de mensagens não confirmadas que o consumer pode
            // receber.
            channel.basicQos(2);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                String videoId = null;
                String jobId = null;

                try {
                    // parse JSON
                    JsonNode root = MAPPER.readTree(raw);
                    videoId = root.path("videoId").asText(null);

                    // determinar jobId:
                    String amqpMsgId = delivery.getProperties() != null ? delivery.getProperties().getMessageId()
                            : null;
                    if (amqpMsgId != null && !amqpMsgId.isBlank()) {
                        jobId = amqpMsgId;
                    } else {
                        jobId = "thumb-" + videoId + "-" + Instant.now().toEpochMilli();
                    }

                    LOGGER.info("[x] Mensagem recebida: videoId='{}' jobId='{}'", videoId, jobId);

                    // Idempotência: se já processado, ack e sai
                    if (repo.isFinished(jobId)) {
                        LOGGER.info("[i] Job já processado (jobId={}) -> ack", jobId);
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    // determina uploads dir (env UPLOADS_DIR ou fallback para workingDir/uploads)
                    String uploadsDir = System.getenv().getOrDefault("UPLOADS_DIR",
                            System.getProperty("user.dir") + File.separator + "uploads");
                    File videoFile = new File(uploadsDir, videoId + ".mp4");

                    LOGGER.info("     Procurando arquivo em: {}", videoFile.getAbsolutePath());

                    if (!videoFile.exists()) {
                        throw new FileNotFoundException(
                                "Arquivo de vídeo não encontrado: " + videoFile.getAbsolutePath());
                    }

                    // cria pasta outputs (thumbnails)
                    File outDir = new File(System.getProperty("user.dir"), "outputs");
                    if (!outDir.exists() && !outDir.mkdirs()) {
                        LOGGER.warn("Não foi possível criar pasta de outputs: {}", outDir.getAbsolutePath());
                    }
                    File thumbnailFile = new File(outDir, videoId + ".jpg");

                    // calcula frame do meio usando demuxer
                    int frameNumberToGrab;
                    try (SeekableByteChannel seekChan = NIOUtils.readableChannel(videoFile)) {
                        MP4Demuxer demuxer = MP4Demuxer.createMP4Demuxer(seekChan);
                        int totalFrames = demuxer.getVideoTrack().getMeta().getTotalFrames();
                        if (totalFrames <= 0) {
                            throw new IOException("Vídeo sem frames ou corrompido: " + videoFile.getName());
                        }
                        frameNumberToGrab = Math.max(0, totalFrames / 2);
                    }

                    // extrai frame com FrameGrab
                    Picture picture = FrameGrab.getFrameFromFile(videoFile, frameNumberToGrab);
                    if (picture == null) {
                        throw new IOException(
                                "Falha ao extrair frame " + frameNumberToGrab + " de " + videoFile.getName());
                    }

                    BufferedImage bufferedImage = AWTUtil.toBufferedImage(picture);
                    ImageIO.write(bufferedImage, "jpg", thumbnailFile);

                    String thumbEventJson = MAPPER.createObjectNode()
                            .put("event", "ThumbnailCreated")
                            .put("jobId", jobId)
                            .put("videoId", videoId)
                            .put("thumbnail", thumbnailFile.getAbsolutePath())
                            .toString();

                    AMQP.BasicProperties pubProps = new AMQP.BasicProperties.Builder()
                            .contentType("application/json")
                            .messageId(jobId) // importante: ajuda idempotência/rastreamento
                            .correlationId(videoId)
                            .deliveryMode(2)
                            .build();

                    // publica no exchange central (video.exchange) com routing key
                    // thumbnail.created
                    channel.basicPublish("video.exchange", "thumbnail.created", pubProps,
                            thumbEventJson.getBytes(StandardCharsets.UTF_8));

                    LOGGER.info("[→] Published thumbnail.created for videoId={} jobId={}", videoId, jobId);

                    // marca como finalizado ANTES do ack (persistência do state)
                    repo.markFinished(jobId);

                    LOGGER.info("[✔] Thumbnail gerado: {} (jobId={})", thumbnailFile.getAbsolutePath(), jobId);

                    // ack
                    channel.basicAck(deliveryTag, false);

                } catch (Exception e) {
                    // log detalhado e enviar para DLQ
                    if (videoId != null) {
                        LOGGER.error(
                                "[!] Falha processando thumbnail para videoId='{}' jobId='{}'. Enviando para DLQ. Erro: {}",
                                videoId, jobId, e.getMessage(), e);
                    } else {
                        LOGGER.error("[!] Falha processando mensagem (raw='{}'). Enviando para DLQ. Erro: {}",
                                raw, e.getMessage(), e);
                    }
                    try {
                        channel.basicNack(deliveryTag, false, false);
                    } catch (IOException ioe) {
                        LOGGER.error("Erro ao enviar nack: {}", ioe.getMessage(), ioe);
                    }
                }
            };

            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
            });

            // manter app vivo
            System.in.read();
        }
    }
}
