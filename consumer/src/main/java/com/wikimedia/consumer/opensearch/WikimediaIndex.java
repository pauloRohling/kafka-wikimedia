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
        final var getIndexRequest = new GetIndexRequest(Indexes.RECENT_CHANGE);

        final var exists = this.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            log.info("Index '{}' already exists", Indexes.RECENT_CHANGE);
            return;
        }

        final var createIndexRequest = new CreateIndexRequest(Indexes.RECENT_CHANGE);
        this.client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.info("Index '{}' created successfully", Indexes.RECENT_CHANGE);
    }
}
