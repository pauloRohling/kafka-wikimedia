package com.wikimedia.producer.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
@Slf4j
public class WikimediaEventHandler {

    private final GzipWikimediaEventProducer gzipWikimediaEventProducer;
    private final Lz4WikimediaEventProducer lz4WikimediaEventProducer;
    private final SnappyWikimediaEventProducer snappyWikimediaEventProducer;
    private final ZstdWikimediaEventProducer zstdWikimediaEventProducer;
    private final WikimediaEventProducer wikimediaEventProducer;
    private final ObjectMapper objectMapper;

    private final WebClient webClient = WebClient.builder()
        .baseUrl("https://stream.wikimedia.org/v2/stream/recentchange")
        .build();

    @PostConstruct
    public void onEvent() {
        this.webClient.get()
            .retrieve()
            .bodyToFlux(String.class)
            .take(Duration.ofSeconds(10))
            .subscribe(event -> {
                final var key = this.getKey(event);
                log.info("Received an event: key = {}; value = {}", key, event);

                this.wikimediaEventProducer.produce(key, event);
                this.gzipWikimediaEventProducer.produce(key, event);
                this.lz4WikimediaEventProducer.produce(key, event);
                this.snappyWikimediaEventProducer.produce(key, event);
                this.zstdWikimediaEventProducer.produce(key, event);
            });
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
