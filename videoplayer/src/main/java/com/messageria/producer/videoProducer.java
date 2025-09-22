package com.messageria.producer;

import com.messageria.config.rabbitMQConfig;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class VideoProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(VideoProducer.class);
    private final static String EXCHANGE_NAME = "video.exchange";

    public void publishVideo(String videoId) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        try (Connection connection = config.createConnection();
                Channel channel = connection.createChannel()) {

            String mediaId = "video-12345";
            String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()); // "2025-09-20T18:34:00Z"
            String jobId = mediaId + "-" + timestamp.replace(":", "").replace(".", ""); // sanitize
            // ex: video-12345-20250920T183400Z

            // A mensagem é publicada diretamente no exchange, não na fila
            String inputUrl = "uploads/" + videoId + ".mp4"; 
            String message = "{\"jobId\": \"" + jobId + "\", \"videoId\": \"" + videoId + "\", \"inputUrl\": \"" + inputUrl + "\"}";
            channel.basicPublish(EXCHANGE_NAME, "video.created", null, message.getBytes("UTF-8"));

            LOGGER.info(" [x] Mensagem publicada para o vídeo: '{}'", videoId);
        }
    }

    public static void main(String[] args) throws Exception {
        // Exemplo: Simular o upload de um vídeo com um ID único
        new VideoProducer().publishVideo("video-12345");
    }
}