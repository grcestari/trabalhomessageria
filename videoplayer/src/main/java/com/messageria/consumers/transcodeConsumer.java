package com.messageria.consumers;

import com.rabbitmq.client.*;
import com.messageria.config.rabbitMQConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class transcodeConsumer {
    private final static String QUEUE_NAME = "transcode.queue";

    public static void main(String[] args) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
            // A declaração da fila é removida, pois é gerenciada centralmente pelo rabbitMQConfig.setupTopology()
            
        System.out.println(" [*] Transcode Consumer aguardando mensagens.");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, StandardCharsets.UTF_8);
                    System.out.println(" [x] Recebida mensagem para transcodificação: '" + message + "'");

                    // Lógica de processamento do vídeo (transcodificação)
                    // Simulando um processamento bem-sucedido.
                    // Em um caso real, a variável processingIsSuccessful dependeria do resultado da sua lógica.
                    boolean processingIsSuccessful = true;

                    if (processingIsSuccessful) {
                        System.out.println(" [✔] Transcodificação concluída com sucesso para: '" + message + "'");
                        channel.basicAck(envelope.getDeliveryTag(), false); // Confirma o sucesso.
                    } else {
                        System.out.println(" [!] Falha na transcodificação para: '" + message + "'. Enviando para DLQ.");
                        channel.basicNack(envelope.getDeliveryTag(), false, false); // Rejeita e envia para a DLQ.
                    }
                } catch (Exception e) {
                    System.err.println(" [!] Erro inesperado ao processar mensagem. Enviando para DLQ. Erro: " + e.getMessage());
                    channel.basicNack(envelope.getDeliveryTag(), false, false); // Rejeita em caso de exceção.
                }
            }
        };
            channel.basicConsume(QUEUE_NAME, false, consumer); // "false" desativa o auto-ack

            // Mantém o consumidor rodando
            System.in.read();
        }
    }
}