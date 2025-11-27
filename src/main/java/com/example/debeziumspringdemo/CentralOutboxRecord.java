package com.example.debeziumspringdemo;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "central_outbox")
@Data
@Builder
@ToString
public class CentralOutboxRecord {

  @Id
  private String id;
  private String aggregateId;
  private String aggregateType;
  private String payload;
  @Indexed
  private Instant timestamp;
  private String sourceDbName;
  private String sourceCollectionName;
}
