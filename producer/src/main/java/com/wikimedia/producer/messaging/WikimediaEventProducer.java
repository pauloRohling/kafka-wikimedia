package com.wikimedia.producer.messaging;

import jakarta.annotation.PreDestroy;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class WikimediaEventProducer {

    private final KafkaProducer<String, String> kafkaProducer;

    public WikimediaEventProducer(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        final var properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void produce(String key, String value) {
        this.kafkaProducer.send(new ProducerRecord<>(Topics.RECENT_CHANGE, key, value));
    }

    @PreDestroy
    public void shutdown() {
        this.kafkaProducer.close();
        log.info("Closed topic '{}'", Topics.RECENT_CHANGE);
    }
}
