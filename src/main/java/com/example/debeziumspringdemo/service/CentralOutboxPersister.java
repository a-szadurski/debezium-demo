package com.example.debeziumspringdemo.service;

import com.example.debeziumspringdemo.domain.CentralOutboxRecord;
import com.example.debeziumspringdemo.domain.CentralOutboxRepository;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CentralOutboxPersister {

  private final CentralOutboxRepository centralOutboxRepository;

  @Retryable(
      maxAttempts = 5,
      backoff = @Backoff(multiplier = 2.0),
      retryFor = {Exception.class},
      recover = "recover"
  )
  public void saveAll(Collection<CentralOutboxRecord> outboxRecord) {
    centralOutboxRepository.saveAll(outboxRecord);
  }

  @Recover
  public void recover(Exception e, Collection<CentralOutboxRecord> records) {
    //some recover logic
    throw new RuntimeException("Could not persist batch after retries", e); //dummy exception
  }
}
