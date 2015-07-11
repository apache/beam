/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker.logging;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.testing.RestoreDataflowLoggingMDC;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/** Unit tests for {@link DataflowWorkerLoggingHandler}. */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingHandlerTest {
  @Rule public TestRule restoreMDC = new RestoreDataflowLoggingMDC();

  /** Returns the json-escaped string for the platform specific line separator.*/
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
      throw Throwables.propagate(e);
    }
  }

  // Typically \n or \r\n
  private static String escapedNewline = escapeNewline();

  /**
   * Encodes a LogRecord into a Json string.
   */
  private static String createJson(LogRecord record) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataflowWorkerLoggingHandler handler = new DataflowWorkerLoggingHandler(output);
    // Format the record as JSON.
    handler.publish(record);
    // Decode the binary output as UTF-8 and return the generated string.
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
  }

  @Test
  public void testWithUnsetValuesInMDC() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", null)));
  }

  @Test
  public void testWithAllValuesInMDC() throws IOException {
    DataflowWorkerLoggingMDC.setJobId("testJobId");
    DataflowWorkerLoggingMDC.setStageName("testStage");
    DataflowWorkerLoggingMDC.setStepName("testStep");
    DataflowWorkerLoggingMDC.setWorkerId("testWorkerId");
    DataflowWorkerLoggingMDC.setWorkId("testWorkId");

    createLogRecord("test.message", null);
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"stage\":\"testStage\",\"step\":\"testStep\",\"worker\":\"testWorkerId\","
            + "\"work\":\"testWorkId\",\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", null)));
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
        createJson(createLogRecord("test.message", null)));
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
        createJson(createLogRecord(null, createThrowable())));
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
        createJson(createLogRecord(null, null)));
  }

  /**
   * @return A throwable with a fixed stack trace.
   */
  private Throwable createThrowable() {
    Throwable throwable = new Throwable("exception.test.message");
    throwable.setStackTrace(new StackTraceElement[]{
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
   * @return A {@link LogRecord} with the given message and throwable.
   */
  private LogRecord createLogRecord(String message, Throwable throwable) {
    LogRecord logRecord = new LogRecord(Level.INFO, message);
    logRecord.setLoggerName("LoggerName");
    logRecord.setMillis(1L);
    logRecord.setThreadID(2);
    logRecord.setThrown(throwable);
    return logRecord;
  }
}
