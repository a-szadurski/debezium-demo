package com.example.debeziumspringdemo.service;

import com.example.debeziumspringdemo.domain.CentralOutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ChangeEventReader {

  private final ObjectMapper objectMapper;

  public CentralOutboxRecord readOutboxRecord(String sourceRecordValue) throws Exception {
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
}
