package com.example.debeziumspringdemo.configuration;

import java.io.IOException;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@RequiredArgsConstructor
public class DebeziumConnectorConfig {

  private final DebeziumConfigurationProperties debeziumConfigurationProperties;

  @Bean
  public io.debezium.config.Configuration debeziumMongoConnector() {
    final var debeziumConfigBuilder = io.debezium.config.Configuration.create();
    debeziumConfigurationProperties.getProperties().forEach(debeziumConfigBuilder::with);
    return debeziumConfigBuilder.build();
  }
}