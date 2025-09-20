package com.messageria.consumers;

import com.rabbitmq.client.*;
import com.messageria.config.rabbitMQConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class notificationConsumer {
    private final static String QUEUE_NAME = "notificacao.queue";

    public static void main(String[] args) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
        System.out.println(" [*] Notification Consumer aguardando mensagens.");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, StandardCharsets.UTF_8);
                    System.out.println(" [x] Recebida mensagem para notificação: '" + message + "'");
                    
                    // Lógica de envio de notificação...
                    System.out.println(" [✔] Notificação enviada com sucesso para: '" + message + "'");

                    // Confirma o processamento da mensagem
                    channel.basicAck(envelope.getDeliveryTag(), false);

                } catch (Exception e) {
                    System.err.println(" [!] Falha ao enviar notificação. Mensagem será descartada. Erro: " + e.getMessage());
                    // Rejeita a mensagem. Como não há DLQ para esta fila, ela será descartada.
                    channel.basicNack(envelope.getDeliveryTag(), false, false);
                }
            }
        };
            channel.basicConsume(QUEUE_NAME, false, consumer); // autoAck = false

            System.in.read();
        }
    }
}