package com.messageria.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class videoProducer {
    private final static String EXCHANGE_NAME = "video.exchange";

    public void publishVideo(String videoId) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Altere se necessário

        try (Connection connection = factory.newConnection();
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