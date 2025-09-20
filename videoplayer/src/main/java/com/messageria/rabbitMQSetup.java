package com.messageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class rabbitMQSetup {
    private final static String EXCHANGE_NAME = "video.exchange";
    
    public static void setup() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Altere se necessário

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declara o exchange do tipo topic
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            // Declara e vincula a fila de miniaturas
            channel.queueDeclare("thumbnail.queue", false, false, false, null);
            channel.queueBind("thumbnail.queue", EXCHANGE_NAME, "video.created");

            // Declara e vincula a fila de transcodificação
            channel.queueDeclare("transcode.queue", false, false, false, null);
            channel.queueBind("transcode.queue", EXCHANGE_NAME, "video.created");

            // Declara e vincula a fila de notificação
            channel.queueDeclare("notificacao.queue", false, false, false, null);
            channel.queueBind("notificacao.queue", EXCHANGE_NAME, "video.created");

            System.out.println("Configuração do RabbitMQ concluída.");
        }
    }

    public static void main(String[] args) throws Exception {
        setup();
    }
}
