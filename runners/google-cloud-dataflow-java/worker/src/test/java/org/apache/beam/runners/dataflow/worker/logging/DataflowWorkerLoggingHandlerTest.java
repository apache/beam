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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.NameContextsForTests;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.testing.RestoreDataflowLoggingMDC;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
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
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(output);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    // Format the record as JSON.
    handler.publish(record);
    // Decode the binary output as UTF-8 and return the generated string.
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String createJson(LogRecord record, Formatter formatter) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(output);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    handler.setFormatter(formatter);
    // Format the record as JSON.
    handler.publish(record);
    // Decode the binary output as UTF-8 and return the generated string.
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
  }

  /**
   * Encodes a {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry} into a Json string.
   */
  private static String createJson(BeamFnApi.LogEntry record) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(output);
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(factory, 0);
    // Format the record as JSON.
    handler.publish(record);
    // Decode the binary output as UTF-8 and return the generated string.
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
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
    // We thus expect to see 2 messsages written to 'first' and 1 message to 'second',

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

    createLogRecord("test.message", null /* throwable */);
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
    MDC.put("testMdcKey", "testMdcValue");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"testMdcValue:test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"worker\":\"testWorkerId\",\"work\":\"testWorkId\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", null /* throwable */), customFormatter));
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

  @Test
  public void testBeamFnApiLogEntry() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"worker\":\"testWorkerId\",\"work\":\"1\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogEntry("test.message")));
  }

  @Test
  public void testBeamFnApiLogEntryWithTrace() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"worker\":\"testWorkerId\",\"work\":\"1\",\"logger\":\"LoggerName\","
            + "\"exception\":\"testTrace\"}"
            + System.lineSeparator(),
        createJson(createLogEntry("test.message").toBuilder().setTrace("testTrace").build()));
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

  /**
   * Creates and returns a BeamFnApi.LogEntry with a given message and throwable.
   *
   * @param message The message to place in the {@link
   *     org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry}
   * @return A {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry} with the given
   *     message.
   */
  private BeamFnApi.LogEntry createLogEntry(String message) {
    return BeamFnApi.LogEntry.newBuilder()
        .setLogLocation("LoggerName")
        .setSeverity(BeamFnApi.LogEntry.Severity.Enum.INFO)
        .setMessage(message)
        .setInstructionId("1")
        .setThread("2")
        .setTimestamp(Timestamp.newBuilder().setSeconds(0).setNanos(1 * 1000000))
        .build();
  }
}
