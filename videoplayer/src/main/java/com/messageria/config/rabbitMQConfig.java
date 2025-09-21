package com.messageria.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class rabbitMQConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(rabbitMQConfig.class);
    private final ConnectionFactory factory;

    public rabbitMQConfig() {
        this.factory = new ConnectionFactory();
        factory.setHost("localhost"); // Host do RabbitMQ
        factory.setUsername("user");  // Credenciais de segurança
        factory.setPassword("password");
    }

    public void setupTopology() throws IOException, TimeoutException {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            LOGGER.info("Configurando a topologia do RabbitMQ...");

            // Declaração do Exchange Central do tipo "topic"
            channel.exchangeDeclare("video.exchange", "topic", true);

            // 1. Fila para geração de miniaturas com Dead Letter Queue
            Map<String, Object> thumbArgs = new HashMap<>();
            thumbArgs.put("x-dead-letter-exchange", "dlx.video");
            thumbArgs.put("x-dead-letter-routing-key", "dead.thumbnail");
            channel.queueDeclare("thumbnail.queue", true, false, false, thumbArgs);
            channel.queueBind("thumbnail.queue", "video.exchange", "video.created");

            // 2. Fila para transcodificação com Dead Letter Queue
            Map<String, Object> transcodeArgs = new HashMap<>();
            transcodeArgs.put("x-dead-letter-exchange", "dlx.video");
            transcodeArgs.put("x-dead-letter-routing-key", "dead.transcode");
            channel.queueDeclare("transcode.queue", true, false, false, transcodeArgs);
            channel.queueBind("transcode.queue", "video.exchange", "video.created");

            // 3. Fila para notificação (não precisa de DLQ neste exemplo, pois falhas de notificação não são críticas)
            channel.queueDeclare("notificacao.queue", true, false, false, null);
            channel.queueBind("notificacao.queue", "video.exchange", "video.created");

            // 4. Configuração do Dead Letter Exchange e suas Filas
            channel.exchangeDeclare("dlx.video", "direct", true); // Exchange DLX

            channel.queueDeclare("dlq.thumbnail.queue", true, false, false, null);
            channel.queueBind("dlq.thumbnail.queue", "dlx.video", "dead.thumbnail");

            channel.queueDeclare("dlq.transcode.queue", true, false, false, null);
            channel.queueBind("dlq.transcode.queue", "dlx.video", "dead.transcode");

            LOGGER.info("Configuração da topologia do RabbitMQ concluída com sucesso.");
        }
    }

    // Método para criar uma conexão, a ser usado por Produtores e Consumidores
    public Connection createConnection() throws IOException, TimeoutException {
        return factory.newConnection();
    }

    // Main para executar o setup da topologia de forma independente
    public static void main(String[] args) throws IOException, TimeoutException {
        rabbitMQConfig config = new rabbitMQConfig();
        config.setupTopology();
    }
}