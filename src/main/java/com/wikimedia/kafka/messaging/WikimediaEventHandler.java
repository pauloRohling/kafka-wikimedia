package com.wikimedia.kafka.messaging;

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

    private final WikimediaEventProducer wikimediaEventProducer;

    private final WebClient webClient = WebClient.builder()
        .baseUrl("https://stream.wikimedia.org/v2/stream/recentchange")
        .build();

    @PostConstruct
    public void onEvent() {
        this.webClient.get()
            .retrieve()
            .bodyToFlux(String.class)
            .take(Duration.ofSeconds(5))
            .subscribe(event -> {
                log.info("Received an event: {}", event);
                this.wikimediaEventProducer.produce(event);
            });
    }
}
