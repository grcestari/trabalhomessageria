package com.messageria.producer;

import com.messageria.config.rabbitMQConfig;

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

            // A mensagem é publicada diretamente no exchange, não na fila
            String inputUrl = "uploads/" + videoId + ".mp4"; 
            String message = "{\"videoId\": \"" + videoId + "\", \"inputUrl\": \"" + inputUrl + "\"}";
            channel.basicPublish(EXCHANGE_NAME, "video.created", null, message.getBytes("UTF-8"));

            LOGGER.info(" [x] Mensagem publicada para o vídeo: '{}'", videoId);
        }
    }

    public static void main(String[] args) throws Exception {
        // Exemplo: Simular o upload de um vídeo com um ID único
        new VideoProducer().publishVideo("video-12345");
    }
}