package com.github.marschall.jfr.opentelemetry.bridge;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.exporter.logging.SystemOutLogRecordExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import jdk.jfr.consumer.EventStream;

class JfrOpentelemetryBridgeTests {

  private static OpenTelemetrySdk otelSdk;

  @BeforeAll
  static void setUpOtel() {
    otelSdk = OpenTelemetrySdk.builder()
                              .setLoggerProvider(SdkLoggerProvider.builder()
                                      .addLogRecordProcessor(SimpleLogRecordProcessor.create(SystemOutLogRecordExporter.create()))
                                      .build())
                              .build();
  }

  @AfterAll
  static void stopOtel() {
    otelSdk.close();
  }

  private Logger logger;

  @BeforeEach
  void setUp() {
    this.logger = otelSdk.getLogsBridge().get(this.getClass().getSimpleName());
  }

  @Test
  void readFile() throws IOException {
    Path recordingFile = Paths.get("src/test/resources/recording.jfr");
    JfrOpentelemetryBridge.logEventsTo(recordingFile, this.logger);
  }

  @Test
  void streamFile() throws IOException, InterruptedException {
    Path recordingFile = Paths.get("src/test/resources/recording.jfr");
    try(EventStream eventStream = JfrOpentelemetryBridge.streamEventsTo(recordingFile, this.logger)) {
      eventStream.awaitTermination();
    }
  }

}
