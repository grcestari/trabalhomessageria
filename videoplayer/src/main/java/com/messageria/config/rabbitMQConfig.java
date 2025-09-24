package com.messageria.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class rabbitMQConfig {

    private final ConnectionFactory factory;

    public rabbitMQConfig() {
        this.factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("user");
        factory.setPassword("password");
    }

    public void setupTopology() {
        try (Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {

            System.out.println("Configurando RabbitMQ");

            channel.exchangeDeclare("video.exchange", "topic", true);

            // Thumbnail
            Map<String, Object> thumbArgs = new HashMap<>();
            thumbArgs.put("x-dead-letter-exchange", "dlx.video");
            thumbArgs.put("x-dead-letter-routing-key", "dead.thumbnail");
            channel.queueDeclare("thumbnail.queue", true, false, false, thumbArgs);
            channel.queueBind("thumbnail.queue", "video.exchange", "video.created");

            // Transcode
            Map<String, Object> transcodeArgs = new HashMap<>();
            transcodeArgs.put("x-dead-letter-exchange", "dlx.video");
            transcodeArgs.put("x-dead-letter-routing-key", "dead.transcode");
            channel.queueDeclare("transcode.queue", true, false, false, transcodeArgs);
            channel.queueBind("transcode.queue", "video.exchange", "video.created");

            // Notificação
            channel.queueDeclare("notificacao.queue", true, false, false, null);
            channel.queueBind("notificacao.queue", "video.exchange", "transcode.created");
            channel.queueBind("notificacao.queue", "video.exchange", "thumbnail.created");

            channel.exchangeDeclare("dlx.video", "direct", true);

            channel.queueDeclare("dlq.thumbnail.queue", true, false, false, null);
            channel.queueBind("dlq.thumbnail.queue", "dlx.video", "dead.thumbnail");

            channel.queueDeclare("dlq.transcode.queue", true, false, false, null);
            channel.queueBind("dlq.transcode.queue", "dlx.video", "dead.transcode");

            System.out.println("Configuração do RabbitMQ concluída com sucesso.");
        } catch (Exception e) {
            System.out.println("Erro ao configurar RabbitMQ: " + e.getMessage());
        }
    }

    public Connection createConnection() throws IOException, TimeoutException {
        return factory.newConnection();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        rabbitMQConfig config = new rabbitMQConfig();
        config.setupTopology();
    }
}