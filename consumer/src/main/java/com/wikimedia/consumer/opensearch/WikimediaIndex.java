package com.wikimedia.consumer.opensearch;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class WikimediaIndex {

    private final RestHighLevelClient client;

    @PostConstruct
    public void init() throws IOException {
        final var getIndexRequest = new GetIndexRequest("wikimedia");

        final var exists = this.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            log.info("Index 'wikimedia' already exists");
            return;
        }

        final var createIndexRequest = new CreateIndexRequest("wikimedia");
        this.client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.info("Index 'wikimedia' created successfully");
    }
}
