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
import java.util.LinkedList;
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

  public DebeziumSourceEventListener(Configuration mongodbConnector, CentralOutboxPersister centralOutboxPersister, ObjectMapper objectMapper) {
    this.centralOutboxPersister = centralOutboxPersister;
    this.objectMapper = objectMapper;
    this.executor = Executors.newSingleThreadExecutor();

    this.debeziumEngine = DebeziumEngine.create(Json.class)
        .using(mongodbConnector.asProperties())
        .notifying(this::handleBatchSafely)
        .using((success, message, error) -> {
          log.info("Debezium engine completed. success={} message={}", success, message);
          if (error != null) {
            log.error("Debezium engine error", error);
          }
        })
        .build();
  }

  private void handleBatchSafely(List<ChangeEvent<String, String>> events, RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {

    List<CentralOutboxRecord> outboxRecords = new LinkedList<>();

    for (ChangeEvent<String, String> event : events) {
      var sourceRecordValue = event.value();
      try {
        outboxRecords.add(readOutboxRecord(sourceRecordValue));
      } catch (Exception e) {
        log.error("Error processing record: {}", sourceRecordValue, e);
        committer.markProcessed(event);
      }
    }
    try {
      centralOutboxPersister.saveAll(outboxRecords);
      for (ChangeEvent<String, String> event : events) {
        committer.markProcessed(event);
      }
      committer.markBatchFinished();
    } catch (Exception e) {
      log.error("Error saving central outbox records", e);
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
    this.executor.execute(debeziumEngine);
  }

  @PreDestroy
  private void stop() throws IOException {
    if (debeziumEngine != null) {
      debeziumEngine.close();
    }
  }

  //AI example for handling mongo errors with batch save
  private void handleBatchSafelyExampleForMongoErrors(
      List<ChangeEvent<String, String>> events,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer
  ) throws InterruptedException {

    List<CentralOutboxRecord> outboxRecords = new LinkedList<>();
    // only events that we actually want to commit if saveAll succeeds
    List<ChangeEvent<String, String>> successfullyParsedEvents = new LinkedList<>();

    for (ChangeEvent<String, String> event : events) {
      String sourceRecordValue = event.value();

      try {
        CentralOutboxRecord record = readOutboxRecord(sourceRecordValue);
        outboxRecords.add(record);
        successfullyParsedEvents.add(event);
      } catch (Exception e) {
        log.error("Error processing record: {}", sourceRecordValue, e);
        // here you can send to DLQ/fallback and *optionally* mark this one processed
        // so it doesn't poison the stream forever:
        // dlqProducer.send(...);
        committer.markProcessed(event);
      }
    }

    try {
      // this is your side-effect: if this fails, we DO NOT advance offsets for the good events
      if (!outboxRecords.isEmpty()) {
        centralOutboxPersister.saveAll(outboxRecords);
      }

      // only after saveAll succeeds, we acknowledge those events
      for (ChangeEvent<String, String> event : successfullyParsedEvents) {
        committer.markProcessed(event);
      }
      committer.markBatchFinished();
    } catch (Exception e) {
      log.error("Failed to persist batch, leaving offsets uncommitted so batch can be retried", e);
      // Important: do NOT call markProcessed / markBatchFinished here.
      // Debezium will not advance the committed offsets for these events.
      // You may want retry/backoff logic or let the engine be restarted.
    }
  }

  //todo AI suggestion - check this
//  private void handleBatchSafelyExample(
//      List<ChangeEvent<String, String>> events,
//      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer
//  ) throws InterruptedException {
//
//    // One task per event, each on its own virtual thread
//    List<Future<Boolean>> futures = new ArrayList<>(events.size());
//
//    for (ChangeEvent<String, String> event : events) {
//      String sourceRecordValue = event.value();
//
//      Future<Boolean> future = virtualThreadPerTaskExecutor.submit(() -> {
//        try {
//          // your existing logic
//          readOutboxRecord(sourceRecordValue);
//          return true;  // success
//        } catch (Exception e) {
//          // keep your logging behaviour
//          log.error("Error processing record: {}", sourceRecordValue, e);
//          //optional DLQ publish here
//          return false; // failure, but we will still commit
//        }
//      });
//
//      futures.add(future);
//    }
//
//    // Wait for all tasks to finish on the Debezium thread
//    for (int i = 0; i < events.size(); i++) {
//      ChangeEvent<String, String> event = events.get(i);
//      Future<Boolean> future = futures.get(i);
//
//      try {
//        // blocks on the virtual thread finishing
//        future.get();
//      } catch (ExecutionException e) {
//        // shouldn't really happen since we catch inside the task,
//        // but just in case, log it and still move on
//        log.error("Unexpected error waiting for record to complete", e);
//      }
//
//      // Always commit the record (same as your original code)
//      committer.markProcessed(event);
//    }
//
//    // commit batch after all records done
//    committer.markBatchFinished();
//  }

}