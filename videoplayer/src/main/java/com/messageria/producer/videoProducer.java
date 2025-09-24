package com.messageria.producer;

import com.messageria.config.rabbitMQConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class VideoProducer {
    private final static String EXCHANGE_NAME = "video.exchange";

    public void publishVideo(String videoId) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        try (Connection connection = config.createConnection();
                Channel channel = connection.createChannel()) {

            String inputUrl = "uploads/" + videoId + ".mp4"; 
            String message = "{\"videoId\": \"" + videoId + "\", \"inputUrl\": \"" + inputUrl + "\"}";
            channel.basicPublish(EXCHANGE_NAME, "video.created", null, message.getBytes("UTF-8"));

            System.out.println( "Mensagem publicada para o vídeo: " + videoId);
        }
    }

    public static void main(String[] args) throws Exception {
        new VideoProducer().publishVideo("video-12345");
    }
}