package com.messageria.consumers;

import com.rabbitmq.client.*;
import com.messageria.config.rabbitMQConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class notificationConsumer {
    private final static String QUEUE_NAME = "notificacao.queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(notificationConsumer.class);

    public static void main(String[] args) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
        LOGGER.info("Notification Consumer aguardando mensagens na fila '{}'.", QUEUE_NAME);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, StandardCharsets.UTF_8);
                    LOGGER.info("Recebida mensagem para notificação: '{}'", message);
                    
                    // Lógica de envio de notificação...
                    LOGGER.info("Notificação enviada com sucesso para: '{}'", message);

                    // Confirma o processamento da mensagem
                    channel.basicAck(envelope.getDeliveryTag(), false);

                } catch (Exception e) {
                    LOGGER.error("Falha ao enviar notificação. Mensagem será descartada.", e);
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