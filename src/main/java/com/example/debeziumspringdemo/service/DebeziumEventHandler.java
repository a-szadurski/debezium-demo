package com.example.debeziumspringdemo.service;

import com.example.debeziumspringdemo.domain.CentralOutboxRecord;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class DebeziumEventHandler {

  private final ChangeEventReader changeEventReader;
  private final CentralOutboxPersister centralOutboxPersister;

  public void handleBatchSafely(List<ChangeEvent<String, String>> events, RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {

    final var toPersist = new LinkedHashMap<ChangeEvent<String, String>, CentralOutboxRecord>();

    for (ChangeEvent<String, String> event : events) {
      var sourceRecordValue = event.value();
      try {
        toPersist.put(event, changeEventReader.readOutboxRecord(sourceRecordValue));
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
}
