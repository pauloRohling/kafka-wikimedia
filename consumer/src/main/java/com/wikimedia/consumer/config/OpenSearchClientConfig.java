package com.wikimedia.consumer.config;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.data.client.orhlc.AbstractOpenSearchConfiguration;
import org.opensearch.data.client.orhlc.ClientConfiguration;
import org.opensearch.data.client.orhlc.RestClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class OpenSearchClientConfig extends AbstractOpenSearchConfiguration {

    @Value("${opensearch.client.connected-to}")
    private String connectedTo;

    @Override
    @Bean
    public RestHighLevelClient opensearchClient() {
        final var clientConfiguration = ClientConfiguration.builder()
            .connectedTo(this.connectedTo)
            .build();
        return RestClients.create(clientConfiguration).rest();
    }
}