package com.wikimedia.consumer.messaging;

import com.wikimedia.consumer.opensearch.Indexes;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class WikimediaConsumer {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final RestHighLevelClient openSearchClient;

    public WikimediaConsumer(RestHighLevelClient openSearchClient) {
        final var properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:29092,localhost:39092,localhost:49092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia.recentchange.consumergroup");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.openSearchClient = openSearchClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void listen() {
        this.kafkaConsumer.subscribe(List.of(Topics.RECENT_CHANGE));

        while (true) {
            final var records = this.kafkaConsumer.poll(Duration.ofMillis(3000));

            StreamSupport.stream(records.spliterator(), false)
                .map(this::toIndexRequest)
                .forEach(request -> this.send(request)
                    .map(DocWriteResponse::getId)
                    .ifPresent(s -> log.info("Sent an event to OpenSearch: {}", s)));
        }
    }

    @PreDestroy
    public void shutdown() {
        this.kafkaConsumer.wakeup();
        this.kafkaConsumer.close();
        log.info("Closed consumer of topic '{}'", Topics.RECENT_CHANGE);
    }

    private IndexRequest toIndexRequest(ConsumerRecord<String, String> record) {
        return new IndexRequest(Indexes.RECENT_CHANGE)
            .source(record.value(), XContentType.JSON)
            .id(record.key());
    }

    private Optional<IndexResponse> send(IndexRequest request) {
        try {
            final var response = this.openSearchClient.index(request, RequestOptions.DEFAULT);
            return Optional.of(response);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
