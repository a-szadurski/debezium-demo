package com.example.debeziumspringdemo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
  private final ExecutorService virtualThreadPerTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();

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

      try {
        readOutboxRecord(sourceRecordValue);

        committer.markProcessed(event);
      } catch (Exception e) {
        log.error("Error processing record: {}", sourceRecordValue, e);
        //save to some fallback table/topic, also catch any errors&handle
        //does fallback error require stopping the server?
        committer.markProcessed(event);
      }
    }
    committer.markBatchFinished();
  }

  private void readOutboxRecord(String sourceRecordValue) throws Exception {
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
        .build();

    log.info("value = '{}'", sourceRecordValue);
    log.info("outboxRecord = '{}'", outboxRecord);
    if (outboxRecord != null && outboxRecord.getAggregateType() != null) {
      centralOutboxPersister.save(outboxRecord);
    } else {
      throw new Exception("Unable to save outbox record");
    }
  }

  @PostConstruct
  private void start() {
    this.executor.execute(debeziumEngine);
  }

  @PreDestroy
  private void stop() throws IOException {
    if (debeziumEngine != null) {
      debeziumEngine.close();
      virtualThreadPerTaskExecutor.shutdown();
    }
  }


  //todo AI suggestion - check this
  private void handleBatchSafelyExample(
      List<ChangeEvent<String, String>> events,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer
  ) throws InterruptedException {

    // One task per event, each on its own virtual thread
    List<Future<Boolean>> futures = new ArrayList<>(events.size());

    for (ChangeEvent<String, String> event : events) {
      String sourceRecordValue = event.value();

      Future<Boolean> future = virtualThreadPerTaskExecutor.submit(() -> {
        try {
          // your existing logic
          readOutboxRecord(sourceRecordValue);
          return true;  // success
        } catch (Exception e) {
          // keep your logging behaviour
          log.error("Error processing record: {}", sourceRecordValue, e);
          //optional DLQ publish here
          return false; // failure, but we will still commit
        }
      });

      futures.add(future);
    }

    // Wait for all tasks to finish on the Debezium thread
    for (int i = 0; i < events.size(); i++) {
      ChangeEvent<String, String> event = events.get(i);
      Future<Boolean> future = futures.get(i);

      try {
        // blocks on the virtual thread finishing
        future.get();
      } catch (ExecutionException e) {
        // shouldn't really happen since we catch inside the task,
        // but just in case, log it and still move on
        log.error("Unexpected error waiting for record to complete", e);
      }

      // Always commit the record (same as your original code)
      committer.markProcessed(event);
    }

    // commit batch after all records done
    committer.markBatchFinished();
  }

}