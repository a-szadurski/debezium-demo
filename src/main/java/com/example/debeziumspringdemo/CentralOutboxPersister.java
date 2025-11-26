package com.example.debeziumspringdemo;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CentralOutboxPersister {

  private final CentralOutboxRepository centralOutboxRepository;

  public void save(CentralOutboxRecord outboxRecord) {
    centralOutboxRepository.save(outboxRecord);
  }

}
