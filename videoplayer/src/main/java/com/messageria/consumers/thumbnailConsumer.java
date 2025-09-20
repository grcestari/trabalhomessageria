package com.messageria.consumers;

import com.rabbitmq.client.*;
import com.messageria.config.rabbitMQConfig;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;
import org.jcodec.api.FrameGrab;
import org.jcodec.common.model.Picture;
import org.jcodec.common.io.NIOUtils;
import org.jcodec.common.io.SeekableByteChannel;
import org.jcodec.containers.mp4.demuxer.MP4Demuxer;
import org.jcodec.scale.AWTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class thumbnailConsumer {
    private final static String QUEUE_NAME = "thumbnail.queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(thumbnailConsumer.class);

    public static void main(String[] args) throws Exception {
        rabbitMQConfig config = new rabbitMQConfig();
        
        try (Connection connection = config.createConnection();
             final Channel channel = connection.createChannel()) {
            
        LOGGER.info("Thumbnail Consumer aguardando mensagens na fila '{}'.", QUEUE_NAME);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String videoId = "";
                try {
                    String jsonMessage = new String(body, StandardCharsets.UTF_8);
                    // Para simplicidade, parseamos o JSON manualmente. Em produção, use uma biblioteca como Gson ou Jackson.
                    videoId = jsonMessage.split("\"")[3];
                    LOGGER.info("Recebida mensagem para gerar thumbnail para o vídeo: '{}'", videoId);

                    // --- Início da Lógica de Geração de Thumbnail ---
                    // Assumindo que os vídeos estão em 'uploads' e as thumbnails irão para 'thumbnails'
                    File videoFile = new File("videoplayer\\src\\main\\java\\com\\messageria\\uploads\\" + videoId + ".mp4");
                    if (!videoFile.exists()) {
                        throw new FileNotFoundException("Arquivo de vídeo não encontrado: " + videoFile.getAbsolutePath());
                    }

                    File thumbnailFolder = new File("thumbnails");
                    if (!thumbnailFolder.exists()) {
                        thumbnailFolder.mkdirs(); // Cria a pasta de thumbnails se não existir
                    }
                    File thumbnailFile = new File(thumbnailFolder, videoId + ".jpg");

                    // Lógica robusta para escolher um frame do meio do vídeo
                    int frameNumberToGrab;
                    try (SeekableByteChannel channel = NIOUtils.readableChannel(videoFile)) {
                        MP4Demuxer demuxer = MP4Demuxer.createMP4Demuxer(channel);
                        int totalFrames = demuxer.getVideoTrack().getMeta().getTotalFrames();
                        if (totalFrames <= 0) {
                            throw new IOException("Vídeo corrompido ou sem frames: " + videoFile.getName());
                        }
                        // Pega um frame do meio do vídeo.
                        frameNumberToGrab = totalFrames / 2;
                    }

                    // Extrai o frame do meio calculado
                    Picture picture = FrameGrab.getFrameFromFile(videoFile, frameNumberToGrab);
                    if (picture == null) {
                        throw new IOException("Falha ao extrair o frame " + frameNumberToGrab + " do vídeo " + videoFile.getName());
                    }

                    BufferedImage bufferedImage = AWTUtil.toBufferedImage(picture);
                    ImageIO.write(bufferedImage, "jpg", thumbnailFile);
                    // --- Fim da Lógica de Geração de Thumbnail ---

                    LOGGER.info("Thumbnail gerado com sucesso em: '{}'", thumbnailFile.getAbsolutePath());
                    channel.basicAck(envelope.getDeliveryTag(), false);
                } catch (Exception e) {
                    LOGGER.error("Falha ao processar thumbnail para o vídeo '{}'. Enviando para DLQ.", videoId, e);
                    channel.basicNack(envelope.getDeliveryTag(), false, false);
                }
            }
        };
            channel.basicConsume(QUEUE_NAME, false, consumer); // autoAck = false

            System.in.read();
        }
    }
}