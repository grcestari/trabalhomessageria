package com.messageria.consumers;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;

public class notificationConsumer {
    private final static String QUEUE_NAME = "notification.queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Notification Consumer aguardando mensagens.");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println(" [x] Recebida mensagem para transcodificação: '" + message + "'");
                // Lógica de transcodificação...
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}