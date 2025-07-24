package com.wikimedia.producer.presentation;

import com.wikimedia.producer.application.WikimediaLoadSimulator;
import com.wikimedia.producer.domain.LoadRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class WikimediaController {

    private final WikimediaLoadSimulator simulator;

    @PostMapping("/events")
    public Flux<String> triggerConsumer(@RequestBody LoadRequest loadRequest) {
        return this.simulator.simulate(loadRequest);
    }
}
