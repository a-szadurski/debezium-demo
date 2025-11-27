package com.example.debeziumspringdemo.configuration;

import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "debezium")
@Data
public class DebeziumConfigurationProperties {

  Map<String, String> properties;
}
