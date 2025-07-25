package com.wikimedia.producer.messaging;

import jakarta.annotation.PreDestroy;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SnappyWikimediaEventProducer {

    private final KafkaProducer<String, String> kafkaProducer;

    public SnappyWikimediaEventProducer() {
        final var properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32768"); // 32KB

        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void produce(String key, String value) {
        this.kafkaProducer.send(new ProducerRecord<>(Topics.SNAPPY_RECENT_CHANGE, key, value));
    }

    @PreDestroy
    public void shutdown() {
        this.kafkaProducer.close();
        log.info("Closed topic '{}'", Topics.SNAPPY_RECENT_CHANGE);
    }
}
