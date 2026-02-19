/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.logging;

import static org.apache.beam.runners.dataflow.worker.NameContextsForTests.nameContextForTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.NameContextsForTests;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingHandler.DirectLoggingThrottler;
import org.apache.beam.runners.dataflow.worker.testing.RestoreDataflowLoggingMDC;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.MDC;

/** Unit tests for {@link DataflowWorkerLoggingHandler}. */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingHandlerTest {
  @Rule public TestRule restoreMDC = new RestoreDataflowLoggingMDC();

  /** Returns the json-escaped string for the platform specific line separator. */
  private static String escapeNewline() {
    try {
      String quoted = new ObjectMapper().writeValueAsString(System.lineSeparator());
      int len = quoted.length();
      if (len < 3 || quoted.charAt(0) != '\"' || quoted.charAt(len - 1) != '\"') {
        return "Failed to escape newline; expected quoted intermediate value";
      }
      // Strip the quotes.
      return quoted.substring(1, len - 1);

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  // Typically \n or \r\n
  private static String escapedNewline = escapeNewline();

  private static class FixedOutputStreamFactory implements Supplier<OutputStream> {
    private OutputStream[] streams;
    private int next = 0;

    public FixedOutputStreamFactory(OutputStream... streams) {
      this.streams = streams;
    }

    @Override
    public OutputStream get() {
      return streams[next++];
    }
  }

  /** Encodes a LogRecord into a Json string. */
  private static String createJson(LogRecord record) throws IOException {
    return createJson(record, null, null);
  }

  private static String createJson(
      LogRecord record, @Nullable Formatter formatter, @Nullable Boolean enableMdc)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(output);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    if (formatter != null) {
      handler.setFormatter(formatter);
    }
    if (enableMdc != null) {
      handler.setLogMdc(enableMdc);
    }
    // Format the record as JSON.
    handler.publish(record);
    // Decode the binary output as UTF-8 and return the generated string.
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
  }

  private static LogEntry createLogEntry(LogRecord record) throws IOException {
    return createLogEntry(record, null, null);
  }

  private static PipelineOptions pipelineOptionsForTest() {
    PipelineOptionsFactory.register(DataflowWorkerHarnessOptions.class);
    return PipelineOptionsFactory.fromArgs(
            "--jobId=testJobId",
            "--jobName=testJobName",
            "--region=testRegion",
            "--project=testProject",
            "--workerId=testWorkerName",
            "--directLoggingCooldownSeconds=10000")
        .create();
  }

  private static LogEntry createLogEntry(
      LogRecord record, @Nullable Formatter formatter, @Nullable Boolean enableMdc)
      throws IOException {
    ByteArrayOutputStream fileOutput = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(fileOutput);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    if (formatter != null) {
      handler.setFormatter(formatter);
    }
    if (enableMdc != null) {
      handler.setLogMdc(enableMdc);
    }
    handler.enableDirectLogging(pipelineOptionsForTest(), Level.SEVERE, (e) -> {});
    return handler.constructDirectLogEntry(
        record, (DataflowExecutionState) ExecutionStateTracker.getCurrentExecutionState());
  }

  private final ExecutionStateTracker tracker = ExecutionStateTracker.newForTest();
  private Closeable trackerCleanup;

  @Before
  public void setUp() {
    trackerCleanup = tracker.activate();
  }

  @After
  public void tearDown() throws IOException {
    trackerCleanup.close();
  }

  @Test
  public void testOutputStreamRollover() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");

    ByteArrayOutputStream first = new ByteArrayOutputStream();
    ByteArrayOutputStream second = new ByteArrayOutputStream();

    LogRecord record = createLogRecord("test.message", null /* throwable */);
    String expected =
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\"}"
            + System.lineSeparator();

    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(first, second);
    DataflowWorkerLoggingHandler handler =
        new DataflowWorkerLoggingHandler(factory, expected.length() + 1 /* sizelimit */);

    // Using |expected|+1 for size limit means that we will rollover after writing 2 log messages.
    // We thus expect to see 2 messages written to 'first' and 1 message to 'second',

    handler.publish(record);
    handler.publish(record);
    handler.publish(record);

    assertEquals(expected + expected, new String(first.toByteArray(), StandardCharsets.UTF_8));
    assertEquals(expected, new String(second.toByteArray(), StandardCharsets.UTF_8));
  }

  @Test
  public void testWithUnsetValuesInMDC() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", null /* throwable */)));
  }

  @Test
  public void testWithAllValuesInMDC() throws IOException {
    DataflowExecutionState state = new TestDataflowExecutionState(nameContextForTest(), "activity");
    tracker.enterState(state);

    String testJobId = "testJobId";
    String testStage = "testStage";
    String testWorkerId = "testWorkerId";
    String testWorkId = "testWorkId";

    DataflowWorkerLoggingMDC.setJobId(testJobId);
    DataflowWorkerLoggingMDC.setStageName(testStage);
    DataflowWorkerLoggingMDC.setWorkerId(testWorkerId);
    DataflowWorkerLoggingMDC.setWorkId(testWorkId);

    assertEquals(
        String.format(
            "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
                + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"%s\","
                + "\"stage\":\"%s\",\"step\":\"%s\",\"worker\":\"%s\","
                + "\"work\":\"%s\",\"logger\":\"LoggerName\"}"
                + System.lineSeparator(),
            testJobId,
            testStage,
            NameContextsForTests.USER_NAME,
            testWorkerId,
            testWorkId),
        createJson(createLogRecord("test.message", null /* throwable */)));
  }

  @Test
  public void testWithMessage() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"worker\":\"testWorkerId\",\"work\":\"testWorkId\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", null /* throwable */)));
  }

  @Test
  public void testWithMessageUsingCustomFormatter() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    Formatter customFormatter =
        new SimpleFormatter() {
          @Override
          public synchronized String formatMessage(LogRecord record) {
            return MDC.get("testMdcKey") + ":" + super.formatMessage(record);
          }
        };
    try (MDC.MDCCloseable ignored = MDC.putCloseable("testMdcKey", "testMdcValue")) {
      assertEquals(
          "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
              + "\"message\":\"testMdcValue:test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
              + "\"worker\":\"testWorkerId\",\"work\":\"testWorkId\",\"logger\":\"LoggerName\"}"
              + System.lineSeparator(),
          createJson(createLogRecord("test.message", null /* throwable */), customFormatter, null));
    }
  }

  @Test
  public void testWithMessageRequiringJulFormatting() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message myFormatString\",\"thread\":\"2\","
            + "\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message {0}", null /* throwable */, "myFormatString")));
  }

  @Test
  public void testWithMessageAndException() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"worker\":\"testWorkerId\",\"work\":\"testWorkId\",\"logger\":\"LoggerName\","
            + "\"exception\":\"java.lang.Throwable: exception.test.message"
            + escapedNewline
            + "\\tat declaringClass1.method1(file1.java:1)"
            + escapedNewline
            + "\\tat declaringClass2.method2(file2.java:1)"
            + escapedNewline
            + "\\tat declaringClass3.method3(file3.java:1)"
            + escapedNewline
            + "\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", createThrowable())));
  }

  @Test
  public void testWithException() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"thread\":\"2\",\"job\":\"testJobId\",\"worker\":\"testWorkerId\","
            + "\"work\":\"testWorkId\",\"logger\":\"LoggerName\","
            + "\"exception\":\"java.lang.Throwable: exception.test.message"
            + escapedNewline
            + "\\tat declaringClass1.method1(file1.java:1)"
            + escapedNewline
            + "\\tat declaringClass2.method2(file2.java:1)"
            + escapedNewline
            + "\\tat declaringClass3.method3(file3.java:1)"
            + escapedNewline
            + "\"}"
            + System.lineSeparator(),
        createJson(createLogRecord(null /* message */, createThrowable())));
  }

  @Test
  public void testWithCustomDataEnabledNoMdc() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord(), null, true));
  }

  @Test
  public void testWithCustomDataDisabledWithMdc() throws IOException {
    MDC.clear();
    try (MDC.MDCCloseable closeable = MDC.putCloseable("key1", "cool value")) {
      assertEquals(
          "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
              + "\"message\":\"test.message\",\"thread\":\"2\",\"logger\":\"LoggerName\"}"
              + System.lineSeparator(),
          createJson(createLogRecord()));
    }
  }

  @Test
  public void testWithCustomDataEnabledWithMdc() throws IOException {
    try (MDC.MDCCloseable ignored = MDC.putCloseable("key1", "cool value");
        MDC.MDCCloseable ignored2 = MDC.putCloseable("key2", "another")) {
      assertEquals(
          "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
              + "\"message\":\"test.message\",\"thread\":\"2\",\"logger\":\"LoggerName\","
              + "\"custom_data\":{\"key1\":\"cool value\",\"key2\":\"another\"}}"
              + System.lineSeparator(),
          createJson(createLogRecord(), null, true));
    }
  }

  @Test
  public void testWithoutExceptionOrMessage() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"thread\":\"2\",\"job\":\"testJobId\",\"worker\":\"testWorkerId\","
            + "\"work\":\"testWorkId\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord(null /* message */, null /* throwable */)));
  }

  // Test the direct logging path except for actually sending it to cloud logging.
  @Test
  public void testDirectLoggingEndToEnd() throws IOException {
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    ByteArrayOutputStream fileOutput = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(fileOutput);
    AtomicReference<LogEntry> capturedEntry = new AtomicReference<>();
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    handler.enableDirectLogging(
        pipelineOptionsForTest(),
        Level.SEVERE,
        (LogEntry e) -> assertNull(capturedEntry.getAndSet(e)));
    handler.publish(createLogRecord("test.message", null /* throwable */));
    assertEquals("", new String(fileOutput.toByteArray(), StandardCharsets.UTF_8));

    LogEntry entry = capturedEntry.get();
    assertNotNull(entry);
    assertEquals(Instant.ofEpochMilli(1), entry.getInstantTimestamp());
    assertEquals(Severity.INFO, entry.getSeverity());

    assertNotNull(entry.getResource());
    assertEquals("dataflow_step", entry.getResource().getType());
    assertEquals(
        ImmutableMap.of(
            "job_name",
            "testJobName",
            "job_id",
            "testJobId",
            "region",
            "testRegion",
            "step_id",
            "",
            "project_id",
            "testProject"),
        entry.getResource().getLabels());

    assertTrue(entry.getPayload() instanceof Payload.JsonPayload);
    Payload.JsonPayload jsonPayload = entry.getPayload();
    Map<String, Object> result = jsonPayload.getDataAsMap();
    assertEquals("test.message", result.get("message"));
    assertEquals("2", result.get("thread"));
    assertEquals("LoggerName", result.get("logger"));
    assertEquals("testWorkerId", result.get("worker"));
    assertEquals("testWorkId", result.get("work"));

    Map<String, String> labels = entry.getLabels();
    assertEquals("testJobId", labels.get("dataflow.googleapis.com/job_id"));
    assertEquals("testJobName", labels.get("dataflow.googleapis.com/job_name"));
    assertEquals("testRegion", labels.get("dataflow.googleapis.com/region"));
    assertEquals("testWorkerName", labels.get("compute.googleapis.com/resource_name"));
    assertEquals("instance", labels.get("compute.googleapis.com/resource_type"));
  }

  private static Object messageInEntry(LogEntry entry) {
    return ((Payload.JsonPayload) entry.getPayload()).getDataAsMap().get("message");
  }

  @Test
  public void testDirectLoggingFallback() throws IOException {
    ByteArrayOutputStream fileOutput = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(fileOutput);
    ConcurrentLinkedDeque<LogEntry> capturedEntries = new ConcurrentLinkedDeque<>();
    final AtomicBoolean failNext = new AtomicBoolean(false);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    Consumer<LogEntry> directLogConsumer =
        (LogEntry e) -> {
          if (failNext.get()) {
            throw new RuntimeException("Failure");
          }
          capturedEntries.add(e);
        };
    handler.enableDirectLogging(pipelineOptionsForTest(), Level.SEVERE, directLogConsumer);
    handler.publish(createLogRecord("test.message", null /* throwable */));
    assertEquals("", new String(fileOutput.toByteArray(), StandardCharsets.UTF_8));
    assertEquals(1, capturedEntries.size());
    assertEquals("test.message", messageInEntry(capturedEntries.getFirst()));
    failNext.set(true);
    capturedEntries.clear();

    // This message should fail to send and be sent to disk.
    handler.publish(createLogRecord("test.message2", null /* throwable */));

    String fileContents = new String(fileOutput.toByteArray(), StandardCharsets.UTF_8);
    String fallbackMsg =
        "\"severity\":\"ERROR\",\"message\":\"Failed to buffer log to send directly to cloud logging, falling back to normal logging path for PT2H46M40S. Consider increasing the buffer size with --workerDirectLoggerBufferByteLimit and --workerDirectLoggerBufferElementLimit.";
    assertTrue(fileContents + "didn't contain" + fallbackMsg, fileContents.contains(fallbackMsg));
    String expected =
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message2\",\"thread\":\"2\",\"logger\":\"LoggerName\"}";
    assertTrue(fileContents + " didn't contain " + expected, fileContents.contains(expected));
    assertEquals(0, capturedEntries.size());
    fileOutput.reset();

    // This message should also be sent to disk because we are backing off.
    failNext.set(false);
    handler.publish(createLogRecord("test.message3", null /* throwable */));
    String expected2 =
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message3\",\"thread\":\"2\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator();
    assertEquals(expected2, new String(fileOutput.toByteArray(), StandardCharsets.UTF_8));
    assertEquals(0, capturedEntries.size());
  }

  @Test
  public void testConstructDirectLogEntryWithoutStage() throws IOException {
    LogEntry entry = createLogEntry(createLogRecord("test.message", null /* throwable */));
    assertEquals(
        Payload.JsonPayload.of(
            ImmutableMap.of("message", "test.message", "thread", "2", "logger", "LoggerName")),
        entry.getPayload());
    assertEquals(Severity.INFO, entry.getSeverity());
    assertNull(entry.getResource());
  }

  @Test
  public void testConstructLogEntryWithAllValuesInMDC() throws IOException {
    DataflowExecutionState state = new TestDataflowExecutionState(nameContextForTest(), "activity");
    tracker.enterState(state);

    String testStage = "testStage";
    String testWorkerId = "testWorkerId";
    String testWorkId = "testWorkId";
    String testJobId = "testJobId";

    DataflowWorkerLoggingMDC.setStageName(testStage);
    DataflowWorkerLoggingMDC.setWorkerId(testWorkerId);
    DataflowWorkerLoggingMDC.setWorkId(testWorkId);
    DataflowWorkerLoggingMDC.setJobId(testJobId);

    LogEntry entry = createLogEntry(createLogRecord("test.message", null /* throwable */));
    assertEquals(
        Payload.JsonPayload.of(
            ImmutableMap.of(
                "message",
                "test.message",
                "thread",
                "2",
                "logger",
                "LoggerName",
                "stage",
                testStage,
                "step",
                NameContextsForTests.USER_NAME,
                "worker",
                testWorkerId,
                "work",
                testWorkId,
                "job",
                testJobId)),
        entry.getPayload());
    assertEquals(Severity.INFO, entry.getSeverity());
    assertEquals("dataflow_step", entry.getResource().getType());
    assertEquals(
        ImmutableMap.of(
            "job_name",
            "testJobName",
            "job_id",
            "testJobId",
            "region",
            "testRegion",
            "step_id",
            NameContextsForTests.USER_NAME,
            "project_id",
            "testProject"),
        entry.getResource().getLabels());
  }

  @Test
  public void testDirectLoggingWithMessageUsingCustomFormatter() throws IOException {
    Formatter customFormatter =
        new SimpleFormatter() {
          @Override
          public synchronized String formatMessage(LogRecord record) {
            return MDC.get("testMdcKey") + ":" + super.formatMessage(record);
          }
        };
    try (MDC.MDCCloseable ignored = MDC.putCloseable("testMdcKey", "testMdcValue")) {
      LogEntry entry =
          createLogEntry(
              createLogRecord("test.message", null /* throwable */), customFormatter, null);
      assertEquals(
          Payload.JsonPayload.of(
              ImmutableMap.of(
                  "message", "testMdcValue:test.message", "thread", "2", "logger", "LoggerName")),
          entry.getPayload());
    }
  }

  @Test
  public void testDirectLoggingWithMessageRequiringJulFormatting() throws IOException {
    LogEntry entry =
        createLogEntry(createLogRecord("test.message {0}", null /* throwable */, "myFormatString"));
    assertEquals(
        Payload.JsonPayload.of(
            ImmutableMap.of(
                "message", "test.message myFormatString", "thread", "2", "logger", "LoggerName")),
        entry.getPayload());
  }

  @Test
  public void testDirectLoggingWithMessageAndException() throws IOException {
    Throwable t = createThrowable();
    LogEntry entry = createLogEntry(createLogRecord("test.message", t));
    assertEquals(
        Payload.JsonPayload.of(
            ImmutableMap.of(
                "message",
                "test.message",
                "thread",
                "2",
                "logger",
                "LoggerName",
                "exception",
                "java.lang.Throwable: exception.test.message"
                    + System.lineSeparator()
                    + "\tat declaringClass1.method1(file1.java:1)"
                    + System.lineSeparator()
                    + "\tat declaringClass2.method2(file2.java:1)"
                    + System.lineSeparator()
                    + "\tat declaringClass3.method3(file3.java:1)"
                    + System.lineSeparator())),
        entry.getPayload());
  }

  @Test
  public void testDirectLoggingWithException() throws IOException {
    Throwable t = createThrowable();
    LogEntry entry = createLogEntry(createLogRecord(null, t));
    assertEquals(
        Payload.JsonPayload.of(
            ImmutableMap.of(
                "thread",
                "2",
                "logger",
                "LoggerName",
                "exception",
                "java.lang.Throwable: exception.test.message"
                    + System.lineSeparator()
                    + "\tat declaringClass1.method1(file1.java:1)"
                    + System.lineSeparator()
                    + "\tat declaringClass2.method2(file2.java:1)"
                    + System.lineSeparator()
                    + "\tat declaringClass3.method3(file3.java:1)"
                    + System.lineSeparator())),
        entry.getPayload());
  }

  @Test
  public void testDirectLoggingWithCustomDataEnabledNoMdc() throws IOException {
    LogEntry entry = createLogEntry(createLogRecord(), null, true);
    assertEquals(
        Payload.JsonPayload.of(
            ImmutableMap.of("message", "test.message", "thread", "2", "logger", "LoggerName")),
        entry.getPayload());
  }

  @Test
  public void testDirectLoggingWithCustomDataDisabledWithMdc() throws IOException {
    MDC.clear();
    try (MDC.MDCCloseable closeable = MDC.putCloseable("key1", "cool value")) {
      LogEntry entry = createLogEntry(createLogRecord());
      assertEquals(
          Payload.JsonPayload.of(
              ImmutableMap.of("message", "test.message", "thread", "2", "logger", "LoggerName")),
          entry.getPayload());
    }
  }

  @Test
  public void testDirectLoggingWithCustomDataEnabledWithMdc() throws IOException {
    MDC.clear();
    try (MDC.MDCCloseable ignored = MDC.putCloseable("key1", "cool value");
        MDC.MDCCloseable ignored2 = MDC.putCloseable("key2", "another")) {
      LogEntry entry = createLogEntry(createLogRecord(), null, true);
      assertEquals(
          Payload.JsonPayload.of(
              ImmutableMap.of(
                  "message",
                  "test.message",
                  "thread",
                  "2",
                  "logger",
                  "LoggerName",
                  "custom_data",
                  ImmutableMap.of("key1", "cool value", "key2", "another"))),
          entry.getPayload());
    }
  }

  @Test
  public void testDirectLoggingWithoutExceptionOrMessage() throws IOException {
    LogEntry entry = createLogEntry(createLogRecord(null, null));
    assertEquals(
        Payload.JsonPayload.of(ImmutableMap.of("thread", "2", "logger", "LoggerName")),
        entry.getPayload());
  }

  @Test
  public void isConfiguredDirectLog() throws IOException {
    ByteArrayOutputStream fileOutput = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(fileOutput);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    handler.enableDirectLogging(pipelineOptionsForTest(), Level.WARNING, (e) -> {});

    // Using the default log level to determine.
    LogRecord record = createLogRecord();
    record.setLevel(Level.WARNING);
    assertFalse(handler.isConfiguredDirectLog(record));
    record.setLevel(Level.SEVERE);
    assertFalse(handler.isConfiguredDirectLog(record));
    record.setLevel(Level.INFO);
    assertTrue(handler.isConfiguredDirectLog(record));
    record.setLevel(Level.FINE);
    assertTrue(handler.isConfiguredDirectLog(record));

    // Using an override to determine
    record.setLevel(Level.INFO);
    record.setResourceBundle(
        DataflowWorkerLoggingHandler.resourceBundleForNonDirectLogLevelHint(Level.INFO));
    assertFalse(handler.isConfiguredDirectLog(record));
    record.setResourceBundle(
        DataflowWorkerLoggingHandler.resourceBundleForNonDirectLogLevelHint(Level.FINE));
    assertFalse(handler.isConfiguredDirectLog(record));
    record.setResourceBundle(
        DataflowWorkerLoggingHandler.resourceBundleForNonDirectLogLevelHint(Level.WARNING));
    assertTrue(handler.isConfiguredDirectLog(record));
  }

  @Test
  public void testDirectLoggingThrottler() {
    AtomicReference<Instant> fakeNow = new AtomicReference<>(Instant.EPOCH);
    DirectLoggingThrottler throttler = new DirectLoggingThrottler(fakeNow::get);
    assertTrue(throttler.shouldAttemptDirectLog());

    throttler.setCooldownDuration(Duration.ofSeconds(10));

    // First failure should trigger cooldown and return a message.
    assertNotNull(throttler.noteDirectLoggingEnqueueFailure());
    // Subsequent failures during cooldown should not return a message.
    assertNull(throttler.noteDirectLoggingEnqueueFailure());
    assertFalse(throttler.shouldAttemptDirectLog());

    for (int i = 0; i < 9; ++i) {
      fakeNow.set(Instant.ofEpochSecond(i));
      assertFalse(throttler.shouldAttemptDirectLog());
    }
    fakeNow.set(Instant.ofEpochSecond(10));
    assertTrue(throttler.shouldAttemptDirectLog());

    // Note success and ensure we can still log.
    throttler.noteDirectLoggingEnqueueSuccess();
    assertTrue(throttler.shouldAttemptDirectLog());

    fakeNow.set(Instant.ofEpochSecond(400));

    // Fail again, then immediately note success to reset.
    assertNotNull(throttler.noteDirectLoggingEnqueueFailure());
    assertFalse(throttler.shouldAttemptDirectLog());
    throttler.noteDirectLoggingEnqueueSuccess();
    assertTrue(throttler.shouldAttemptDirectLog());

    // Test error path.
    assertNotNull(throttler.noteDirectLoggingError());
    assertFalse(throttler.shouldAttemptDirectLog());
    assertNull(throttler.noteDirectLoggingError());
    for (int i = 0; i < 9; ++i) {
      fakeNow.set(Instant.ofEpochSecond(400 + i));
      assertFalse(throttler.shouldAttemptDirectLog());
    }
    fakeNow.set(Instant.ofEpochSecond(410));
    assertTrue(throttler.shouldAttemptDirectLog());
  }

  /** @return A throwable with a fixed stack trace. */
  private Throwable createThrowable() {
    Throwable throwable = new Throwable("exception.test.message");
    throwable.setStackTrace(
        new StackTraceElement[] {
          new StackTraceElement("declaringClass1", "method1", "file1.java", 1),
          new StackTraceElement("declaringClass2", "method2", "file2.java", 1),
          new StackTraceElement("declaringClass3", "method3", "file3.java", 1),
        });
    return throwable;
  }

  /** Creates and returns a simple test LogRecord. */
  private LogRecord createLogRecord() {
    return createLogRecord("test.message", null);
  }

  /**
   * Creates and returns a LogRecord with a given message and throwable.
   *
   * @param message The message to place in the {@link LogRecord}
   * @param throwable The throwable to place in the {@link LogRecord}
   * @param params A list of parameters to place in the {@link LogRecord}
   * @return A {@link LogRecord} with the given message and throwable.
   */
  private LogRecord createLogRecord(String message, Throwable throwable, Object... params) {
    LogRecord logRecord = new LogRecord(Level.INFO, message);
    logRecord.setLoggerName("LoggerName");
    logRecord.setMillis(1L);
    logRecord.setThreadID(2);
    logRecord.setThrown(throwable);
    logRecord.setParameters(params);
    return logRecord;
  }
}
