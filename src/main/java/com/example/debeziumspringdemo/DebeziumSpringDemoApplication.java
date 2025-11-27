package com.example.debeziumspringdemo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication
@EnableMongoRepositories
@ConfigurationPropertiesScan
public class DebeziumSpringDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(DebeziumSpringDemoApplication.class, args);
  }

  @Bean
  public ObjectMapper objectMapper() {
    final var mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }


}
