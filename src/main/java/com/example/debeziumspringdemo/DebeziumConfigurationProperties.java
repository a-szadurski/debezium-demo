package com.example.debeziumspringdemo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "debezium")
public class DebeziumConfigurationProperties {

}
