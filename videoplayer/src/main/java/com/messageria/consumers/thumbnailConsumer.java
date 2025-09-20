package com.messageria.consumers;

import com.rabbitmq.client.*;
import com.messageria.config.rabbitMQConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class thumbnailConsumer {
    private final static String QUEUE_NAME = "thumbnail.queue";

    public static void main(String[] args) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
        System.out.println(" [*] Thumbnail Consumer aguardando mensagens.");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, StandardCharsets.UTF_8);
                    System.out.println(" [x] Recebida mensagem para gerar thumbnail: '" + message + "'");

                    // Lógica de processamento (geração de thumbnail)
                    boolean processingIsSuccessful = true; // Simulação

                    if (processingIsSuccessful) {
                        System.out.println(" [✔] Thumbnail gerado com sucesso para: '" + message + "'");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    } else {
                        System.out.println(" [!] Falha ao gerar thumbnail para: '" + message + "'. Enviando para DLQ.");
                        channel.basicNack(envelope.getDeliveryTag(), false, false);
                    }
                } catch (Exception e) {
                    System.err.println(" [!] Erro inesperado ao processar thumbnail. Enviando para DLQ. Erro: " + e.getMessage());
                    channel.basicNack(envelope.getDeliveryTag(), false, false);
                }
            }
        };
            channel.basicConsume(QUEUE_NAME, false, consumer); // autoAck = false

            System.in.read();
        }
    }
}