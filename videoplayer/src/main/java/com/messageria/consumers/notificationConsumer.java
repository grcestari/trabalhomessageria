package com.messageria.consumers;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.messageria.config.rabbitMQConfig;
import java.nio.charset.StandardCharsets;

public class notificationConsumer {
    private final static String QUEUE_NAME = "notificacao.queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(notificationConsumer.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Iniciando Notification Consumer...");
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
            LOGGER.info(" [*] Notification Consumer aguardando mensagens. Para sair, pressione CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                Envelope envelope = delivery.getEnvelope();
                try {
                    LOGGER.info(" [x] Recebida mensagem para notificação: '{}'", message);
                    
                    // Lógica de envio de notificação...
                    LOGGER.info(" [✔] Notificação enviada com sucesso para: '{}'", message);

                    channel.basicAck(envelope.getDeliveryTag(), false);
                } catch (Exception e) {
                    LOGGER.error(" [!] Falha ao enviar notificação para '{}'. Mensagem será descartada.", message, e);
                    channel.basicNack(envelope.getDeliveryTag(), false, false);
                }
            };
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {}); // autoAck = false

            System.in.read();
        }
    }
}