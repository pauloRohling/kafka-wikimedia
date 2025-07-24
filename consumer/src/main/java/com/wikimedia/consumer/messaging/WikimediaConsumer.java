package com.wikimedia.consumer.messaging;

import com.wikimedia.consumer.opensearch.Indexes;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.index.IndexResponse.Builder;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class WikimediaConsumer {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final RestHighLevelClient openSearchClient;

    public WikimediaConsumer(RestHighLevelClient openSearchClient) {
        final var properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia.recentchange.consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.openSearchClient = openSearchClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void listen() {
        this.kafkaConsumer.subscribe(List.of(Topics.RECENT_CHANGE));

        final var records = this.kafkaConsumer.poll(Duration.ofMillis(3000));

        Flux.fromIterable(records)
            .map(ConsumerRecord::value)
            .map(this::toIndexRequest)
            .flatMap(this::send)
            .map(DocWriteResponse::getId)
            .doOnNext(id -> log.info("Sent an event to OpenSearch: {}", id))
            .subscribe();
    }

    @PreDestroy
    public void shutdown() {
        this.kafkaConsumer.unsubscribe();
        this.kafkaConsumer.close();
        log.info("Closed consumer of topic '{}'", Topics.RECENT_CHANGE);
    }

    private IndexRequest toIndexRequest(String value) {
        return new IndexRequest(Indexes.RECENT_CHANGE).source(value, XContentType.JSON);
    }

    private Mono<IndexResponse> send(IndexRequest request) {
        return Mono.fromCallable(() -> {
            try {
                return this.openSearchClient.index(request, RequestOptions.DEFAULT);
            } catch (Exception e) {
                final var builder = new Builder();
                builder.setId("none");
                return builder.build();
            }
        });
    }
}
