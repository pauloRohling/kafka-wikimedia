package com.wikimedia.consumer.config;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.data.client.orhlc.AbstractOpenSearchConfiguration;
import org.opensearch.data.client.orhlc.ClientConfiguration;
import org.opensearch.data.client.orhlc.RestClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenSearchClientConfig extends AbstractOpenSearchConfiguration {

    @Override
    @Bean
    public RestHighLevelClient opensearchClient() {
        final var clientConfiguration = ClientConfiguration.builder()
            .connectedTo("localhost:9200")
            .build();
        return RestClients.create(clientConfiguration).rest();
    }
}