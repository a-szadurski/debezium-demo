package com.example.debeziumspringdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableMongoRepositories
@ConfigurationPropertiesScan
@EnableRetry
public class DebeziumSpringDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(DebeziumSpringDemoApplication.class, args);
  }

}
