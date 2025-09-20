package com.messageria.consumers;

import com.rabbitmq.client.*;
import com.messageria.config.rabbitMQConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class transcodeConsumer {
    private final static String QUEUE_NAME = "transcode.queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(transcodeConsumer.class);

    public static void main(String[] args) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
            // A declaração da fila é removida, pois é gerenciada centralmente pelo rabbitMQConfig.setupTopology()
            
        LOGGER.info("Transcode Consumer aguardando mensagens na fila '{}'.", QUEUE_NAME);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, StandardCharsets.UTF_8);
                    LOGGER.info("Recebida mensagem para transcodificação: '{}'", message);

                    // Lógica de processamento do vídeo (transcodificação)
                    // Simulando um processamento bem-sucedido.
                    // Em um caso real, a variável processingIsSuccessful dependeria do resultado da sua lógica.
                    boolean processingIsSuccessful = true;

                    if (processingIsSuccessful) {
                        LOGGER.info("Transcodificação concluída com sucesso para: '{}'", message);
                        channel.basicAck(envelope.getDeliveryTag(), false); // Confirma o sucesso.
                    } else {
                        LOGGER.warn("Falha na transcodificação para: '{}'. Enviando para DLQ.", message);
                        channel.basicNack(envelope.getDeliveryTag(), false, false); // Rejeita e envia para a DLQ.
                    }
                } catch (Exception e) {
                    LOGGER.error("Erro inesperado ao processar mensagem. Enviando para DLQ.", e);
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