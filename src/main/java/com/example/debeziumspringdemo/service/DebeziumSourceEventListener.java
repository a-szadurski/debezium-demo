package com.example.debeziumspringdemo.service;

import com.example.debeziumspringdemo.domain.CentralOutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Json;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DebeziumSourceEventListener {

  private final Executor executor = Executors.newSingleThreadExecutor();
  private final CentralOutboxPersister centralOutboxPersister;
  private final ObjectMapper objectMapper;
  private final Configuration mongodbConnector;
  private final ConfigurableApplicationContext applicationContext;

  private DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;

  private static final int MAX_ENGINE_ERROR_RESTARTS = 5;
  private static final long RESTART_DELAY_MS = 5_000L;
  private final AtomicInteger consecutiveErrorRestarts = new AtomicInteger(0);

  private void handleBatchSafely(List<ChangeEvent<String, String>> events, RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {

    final var toPersist = new LinkedHashMap<ChangeEvent<String, String>, CentralOutboxRecord>();

    for (ChangeEvent<String, String> event : events) {
      var sourceRecordValue = event.value();
      try {
        toPersist.put(event, readOutboxRecord(sourceRecordValue));
      } catch (Exception e) {
        log.error("Error processing record: {}", sourceRecordValue, e);
        committer.markProcessed(event);
      }
    }

    if (toPersist.isEmpty()) {
      // only broken records in this batch → already marked processed
      committer.markBatchFinished();
      return;
    }

    try {
      //this is retryable
      centralOutboxPersister.saveAll(toPersist.values());
      for (ChangeEvent<String, String> event : toPersist.keySet()) {
        committer.markProcessed(event);
      }
      committer.markBatchFinished();
      //and when it fails completely, recover throws some exception
    } catch (RuntimeException fatal) {
      log.error("Fatal rror saving central outbox records", fatal);
      throw fatal; //todo add real exceptions

    }
  }

  private CentralOutboxRecord readOutboxRecord(String sourceRecordValue) throws Exception {
    var root = objectMapper.readTree(sourceRecordValue);
    var debeziumPayload = root.path("payload");
    var source = debeziumPayload.path("source");

    var db = source.path("db").asText();
    var collection = source.path("collection").asText();

    log.info("DB = '{}', collection = '{}'", db, collection);
    var after = debeziumPayload.path("after").asText(null);
    log.info("after = {}", after);
    var afterJson = objectMapper.readTree(after);
    var aggregatetype = afterJson.path("aggregatetype").asText(null);
    var aggregateid = afterJson.path("aggregateid").asText(null);
    var payloadJson = afterJson.path("payload");
    var payload = payloadJson.toString();

    final var outboxRecord = CentralOutboxRecord.builder()
        .aggregateType(aggregatetype)
        .aggregateId(aggregateid)
        .payload(payload)
        .timestamp(Instant.now())
        .sourceDbName(db)
        .sourceCollectionName(collection)
        .build();

    log.info("value = '{}'", sourceRecordValue);
    log.info("outboxRecord = '{}'", outboxRecord);
    return outboxRecord;
  }

  @PostConstruct
  private void start() {
    startEngine();
  }

  @PreDestroy
  private void stop() throws IOException {
    if (debeziumEngine != null) {
      debeziumEngine.close();
    }
  }

  private synchronized void startEngine() {
    this.debeziumEngine = DebeziumEngine.create(Json.class)
        .using(mongodbConnector.asProperties())
        .notifying(this::handleBatchSafely)
        .using((success, message, error) -> {
          log.info("Debezium engine completed. success={} message={}", success, message);
          if (error != null) {
            log.error("Debezium engine error", error);
          }
        })
        .using(this::onEngineCompletion)
        .build();

    executor.execute(debeziumEngine);
  }

  private void onEngineCompletion(boolean success, String message, Throwable error) {
    if (success) {
      consecutiveErrorRestarts.set(0);
      log.info("Debezium engine stopped normally: {}", message);
    } else {
      final int attempt = consecutiveErrorRestarts.getAndIncrement();
      log.error("Debezium engine stopped with ERROR (consecutive error restarts: {}): {}",
          attempt, message, error);

      if (attempt > MAX_ENGINE_ERROR_RESTARTS) {
        log.error("Max Debezium error restarts ({}) exceeded → shutting down Spring app",
            MAX_ENGINE_ERROR_RESTARTS);
        exitApplication();
        return; // don't restart engine anymore, this means something really wrong is going on
      }
    }
    try {
      Thread.sleep(RESTART_DELAY_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Restart delay interrupted, restarting Debezium engine immediately");
    }

    log.info("Restarting Debezium engine...");
    startEngine();
  }

  private void exitApplication() {
    // graceful shutdown + exit code
    int exitCode = SpringApplication.exit(applicationContext, () -> 1);
    System.exit(exitCode);
  }
}