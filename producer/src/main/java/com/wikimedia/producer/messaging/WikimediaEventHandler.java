package com.wikimedia.producer.messaging;

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

    private final WebClient webClient = WebClient.builder()
        .baseUrl("https://stream.wikimedia.org/v2/stream/recentchange")
        .build();

    @PostConstruct
    public void onEvent() {
        this.webClient.get()
            .retrieve()
            .bodyToFlux(String.class)
            .take(Duration.ofMinutes(1))
            .subscribe(event -> {
                log.info("Received an event: {}", event);
                this.wikimediaEventProducer.produce(event);
                this.gzipWikimediaEventProducer.produce(event);
                this.lz4WikimediaEventProducer.produce(event);
                this.snappyWikimediaEventProducer.produce(event);
                this.zstdWikimediaEventProducer.produce(event);
            });
    }
}
