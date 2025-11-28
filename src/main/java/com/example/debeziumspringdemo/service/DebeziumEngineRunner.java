package com.example.debeziumspringdemo.service;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.io.IOException;
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
public class DebeziumEngineRunner {

  private final Executor executor = Executors.newSingleThreadExecutor();

  private final Configuration mongodbConnector;
  private final ConfigurableApplicationContext applicationContext;
  private final DebeziumEventHandler debeziumEventHandler;

  private DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;

  private static final int MAX_ENGINE_ERROR_RESTARTS = 3;
  private static final long RESTART_DELAY_MS = 5_000L;
  private final AtomicInteger consecutiveErrorRestarts = new AtomicInteger(0);

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
        .notifying(debeziumEventHandler::handleBatchSafely)
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