package com.messageria.consumers;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.messageria.config.rabbitMQConfig;
import java.nio.charset.StandardCharsets;

public class transcodeConsumer {
    private final static String QUEUE_NAME = "transcode.queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(transcodeConsumer.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Iniciando Transcode Consumer...");
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
            LOGGER.info(" [*] Transcode Consumer aguardando mensagens. Para sair, pressione CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                Envelope envelope = delivery.getEnvelope();
                try {
                    LOGGER.info(" [x] Recebida mensagem para transcodificação: '{}'", message);

                    // Lógica de processamento do vídeo (transcodificação)
                    // Simulando um processamento bem-sucedido.
                    // Em um caso real, a variável processingIsSuccessful dependeria do resultado da sua lógica.
                    boolean processingIsSuccessful = true;

                    if (processingIsSuccessful) { // Simulação de sucesso
                        LOGGER.info(" [✔] Transcodificação concluída com sucesso para: '{}'", message);
                        channel.basicAck(envelope.getDeliveryTag(), false); // Confirma o sucesso.
                    } else {
                        LOGGER.warn(" [!] Falha na transcodificação para: '{}'. Enviando para DLQ.", message);
                        channel.basicNack(envelope.getDeliveryTag(), false, false); // Rejeita e envia para a DLQ.
                    }
                } catch (Exception e) {
                    LOGGER.error(" [!] Erro inesperado ao processar mensagem '{}'. Enviando para DLQ.", message, e);
                    channel.basicNack(envelope.getDeliveryTag(), false, false); // Rejeita em caso de exceção.
                }
            };
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {}); // "false" desativa o auto-ack

            // Mantém o consumidor rodando
            System.in.read();
        }
    }
}