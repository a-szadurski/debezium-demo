package com.example.debeziumspringdemo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DebeziumSourceEventListener {

  private final Executor executor;
  private final DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;
  private final CentralOutboxPersister centralOutboxPersister;
  private final ObjectMapper objectMapper;

  public DebeziumSourceEventListener(Configuration mongodbConnector, CentralOutboxPersister centralOutboxPersister) {
    this.centralOutboxPersister = centralOutboxPersister;
    this.executor = Executors.newSingleThreadExecutor();
    this.debeziumEngine = DebeziumEngine.create(Json.class)
        .using(mongodbConnector.asProperties())
        .notifying(this::handleBatchSafely)
        .build();
    objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private void handleBatchSafely(
      List<ChangeEvent<String, String>> events,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer
  ) throws InterruptedException {

    for (ChangeEvent<String, String> event : events) {
      String sourceRecordValue = event.value();
      CentralOutboxRecord outboxRecord = readOutboxRecord(sourceRecordValue);

      committer.markProcessed(event);

    }
    committer.markBatchFinished();
  }

  private void handleChangeEvent(ChangeEvent<String, String> sourceRecordChangeEvent) {

    String sourceRecordValue = sourceRecordChangeEvent.value();

    readOutboxRecord(sourceRecordValue);

  }

  private CentralOutboxRecord readOutboxRecord(String sourceRecordValue) {
    String aggregatetype;
    String aggregateid;
    String payload;
    CentralOutboxRecord outboxRecord = null;
    try {
      JsonNode root = objectMapper.readTree(sourceRecordValue);
      JsonNode debeziumPayload = root.path("payload");

      JsonNode source = debeziumPayload.path("source");

      String db = source.path("db").asText();
      String collection = source.path("collection").asText();

      log.info("DB = '{}', collection = '{}'", db, collection);
      String after = debeziumPayload.path("after").asText(null);
      log.info("after = {}", after);
      JsonNode afterJson = objectMapper.readTree(after);
      aggregatetype = afterJson.path("aggregatetype").asText(null);
      aggregateid = afterJson.path("aggregateid").asText(null);
      JsonNode payloadJson = afterJson.path("payload");
      payload = payloadJson.toString();

      outboxRecord = CentralOutboxRecord.builder()
          .aggregateType(aggregatetype)
          .aggregateId(aggregateid)
          .payload(payload)
          .build();

      log.info("value = '{}'", sourceRecordValue);
      log.info("outboxRecord = '{}'", outboxRecord);
      if (outboxRecord != null && outboxRecord.getAggregateType() != null) {
        centralOutboxPersister.save(outboxRecord);
      }

    } catch (IOException e) {
      // handle parse error
    }
    return outboxRecord;
  }

  @PostConstruct
  private void start() {
    this.executor.execute(debeziumEngine);
  }

  @PreDestroy
  private void stop() throws IOException {
    if (this.debeziumEngine != null) {
      this.debeziumEngine.close();
    }
  }
}