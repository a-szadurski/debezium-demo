package com.example.debeziumspringdemo;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface CentralOutboxRepository extends MongoRepository<CentralOutboxRecord, String> {

}
