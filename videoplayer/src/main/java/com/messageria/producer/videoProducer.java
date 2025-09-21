package com.messageria.producer;

import com.messageria.config.rabbitMQConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class videoProducer {
    private final static String EXCHANGE_NAME = "video.exchange";

    public void publishVideo(String videoId) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        try (Connection connection = config.createConnection();
             Channel channel = connection.createChannel()) {

            // A mensagem é publicada diretamente no exchange, não na fila
            String message = "{\"videoId\": \"" + videoId + "\", \"status\": \"uploaded\"}";
            channel.basicPublish(EXCHANGE_NAME, "video.created", null, message.getBytes("UTF-8"));

            System.out.println(" [x] Mensagem publicada para o vídeo: '" + videoId + "'");
        }
    }

    public static void main(String[] args) throws Exception {
        // Exemplo: Simular o upload de um vídeo com um ID único
        new videoProducer().publishVideo("video-12345");
    }
}