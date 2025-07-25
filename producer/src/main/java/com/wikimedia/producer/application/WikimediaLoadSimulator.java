package com.wikimedia.producer.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.producer.domain.LoadRequest;
import com.wikimedia.producer.messaging.GzipWikimediaEventProducer;
import com.wikimedia.producer.messaging.Lz4WikimediaEventProducer;
import com.wikimedia.producer.messaging.SnappyWikimediaEventProducer;
import com.wikimedia.producer.messaging.WikimediaEventProducer;
import com.wikimedia.producer.messaging.ZstdWikimediaEventProducer;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class WikimediaLoadSimulator {

    private final GzipWikimediaEventProducer gzipWikimediaEventProducer;
    private final Lz4WikimediaEventProducer lz4WikimediaEventProducer;
    private final SnappyWikimediaEventProducer snappyWikimediaEventProducer;
    private final ZstdWikimediaEventProducer zstdWikimediaEventProducer;
    private final WikimediaEventProducer wikimediaEventProducer;
    private final ObjectMapper objectMapper;

    private final WebClient webClient = WebClient.builder()
        .baseUrl("https://stream.wikimedia.org/v2/stream/recentchange")
        .build();

    public Flux<String> simulate(LoadRequest loadRequest) {
        return this.webClient.get()
            .retrieve()
            .bodyToFlux(String.class)
            .take(Duration.ofMillis(loadRequest.millis()))
            .concatMap(event -> Mono.fromCallable(() -> {
                final var key = this.getKey(event);
                log.info("Received an event: key = {}; value = {}", key, event);

                this.wikimediaEventProducer.produce(key, event);
                this.gzipWikimediaEventProducer.produce(key, event);
                this.lz4WikimediaEventProducer.produce(key, event);
                this.snappyWikimediaEventProducer.produce(key, event);
                this.zstdWikimediaEventProducer.produce(key, event);
                return event;
            }));
    }

    private String getKey(String event) {
        JsonNode node;
        try {
            node = this.objectMapper.readTree(event);
        } catch (JsonProcessingException e) {
            return null;
        }

        return node.get("meta").get("id").asText();
    }
}
