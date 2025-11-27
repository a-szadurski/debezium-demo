package com.example.debeziumspringdemo;

import java.io.File;
import java.io.IOException;

import org.springframework.context.annotation.Bean;

import io.debezium.config.Configuration;

@org.springframework.context.annotation.Configuration
public class DebeziumConnectorConfig {
  // based off https://debezium.io/documentation/reference/2.1/connectors/mongodb.html#mongodb-connector-properties
  @Bean
  public Configuration mongodbConnector() throws IOException {
    File offsetStorageTempFile = File.createTempFile("debezium-offsets", ".dat");

    return Configuration.create()
        // engine properties
        .with("name", "sbd-mongodb-3")
        .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
        .with("offset.flush.interval.ms", "10000")
        // connector specific properties
        .with("mongodb.connection.string", "mongodb://admin:password@localhost:27017/?replicaSet=e77b444b513b&authSource=admin")
        .with("topic.prefix", "sbd-mongodb-connector")
        .with("mongodb.user", "admin")
        .with("mongodb.password", "password")
//        .with("mongodb.ssl.enabled", "true") // default false
//        .with("database.include.list", "source") // default empty
        .with("snapshot.delay.ms", "100")
        .with("errors.log.include.messages", "true")
        .with("skipped.operations", "u,d,t")
        .with("max.batch.size", "500")
        .with("snapshot.mode", "initial")
//        .with("filters.match.mode", "literal")
        .with("database.include.list", "inventory,inventory1,inventory2,inventory3,inventory4")
//        .with("collection.include.list", "test.outbox")
        .build();
  }
}