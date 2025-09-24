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

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;

public class ThumbnailConsumer {
    private static final String QUEUE_NAME = "thumbnail.queue";
    private static final String EXCHANGE = "video.exchange";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando Thumbnail Consumer");
        rabbitMQConfig config = new rabbitMQConfig();

        FileJobRepository repo = new FileJobRepository(Path.of("state"));

        try (Connection connection = config.createConnection();
                Channel channel = connection.createChannel()) {

            System.out.println("Thumbnail Consumer aguardando mensagens");

            channel.basicQos(2);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                String videoId = null;
                String jobId = null;
                String inputUrl = null;

                try {
                    JsonNode root = MAPPER.readTree(raw);
                    jobId = root.path("jobId").asText(null);
                    videoId = root.path("videoId").asText(null);
                    inputUrl = root.path("inputUrl").asText(null);

                    if (jobId == null || videoId == null || inputUrl == null) {
                        throw new IllegalArgumentException(
                                "Mensagem inválida, faltando jobId/videoId/inputUrl: " + raw);
                    }

                    jobId = "thumb-" + videoId + "-" + Instant.now().toEpochMilli();
                    
                    System.out.println("Mensagem recebida: videoId='" + videoId + "' jobId='" + jobId + "'");

                    if (repo.isFinished(jobId)) {
                        System.out.println("Job já processado (jobId=" + jobId + ")");
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                
                    File videoFile = new File(inputUrl);
                    System.out.println("Procurando arquivo de vídeo em: " + videoFile.getAbsolutePath());

                    if (!videoFile.exists()) {
                        System.err.println("Arquivo de input não encontrado: " + videoFile.getAbsolutePath());
                        channel.basicNack(deliveryTag, false, false);
                        return;
                    }

                    File outDir = new File("outputs",  videoId + "_thumbnail");
                    if (!outDir.exists()) outDir.mkdirs();
                    File thumbnailFile = new File(outDir, videoId + ".jpg");

                    int frameNumberToGrab;
                    try (SeekableByteChannel seekChan = NIOUtils.readableChannel(videoFile)) {
                        MP4Demuxer demuxer = MP4Demuxer.createMP4Demuxer(seekChan);
                        int totalFrames = demuxer.getVideoTrack().getMeta().getTotalFrames();
                        if (totalFrames <= 0) {
                            throw new IOException("Vídeo sem frames ou corrompido: " + videoFile.getName());
                        }
                        frameNumberToGrab = Math.max(0, totalFrames / 2);
                    }

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
                            .messageId(jobId)
                            .correlationId(videoId)
                            .deliveryMode(2)
                            .build();

                    channel.basicPublish("video.exchange", "thumbnail.created", pubProps,
                            thumbEventJson.getBytes(StandardCharsets.UTF_8));

                    System.out.println("Publicado thumbnail.created,  videoId=" + videoId + " jobId=" + jobId);

                    repo.markFinished(jobId);

                    System.out
                            .println("Thumbnail gerado: " + thumbnailFile.getAbsolutePath() + " (jobId=" + jobId + ")");

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

            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
            });

            System.in.read();
        }
    }
}
