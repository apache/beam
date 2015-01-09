/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.testing.RestoreMappedDiagnosticContext;
import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.MDC;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/** Unit tests for {@link DataflowWorkerLoggingFormatter}. */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingFormatterTest {
  @Rule public TestRule restoreMDC = new RestoreMappedDiagnosticContext();

  @Test
  public void testWithUnsetValuesInMDC() {
    assertEquals(
        "1970-01-01T00:00:00.001Z INFO unknown unknown unknown 2 LoggerName "
        + "test.message" + System.lineSeparator(),
        new DataflowWorkerLoggingFormatter().format(
            createLogRecord("test.message", null)));
  }

  @Test
  public void testWithMessage() {
    MDC.setContextMap(ImmutableMap.<String, String>of(
        "dataflow.jobId", "testJobId",
        "dataflow.workerId", "testWorkerId",
        "dataflow.workId", "testWorkId"));
    assertEquals(
        "1970-01-01T00:00:00.001Z INFO testJobId testWorkerId testWorkId 2 LoggerName "
        + "test.message" + System.lineSeparator(),
        new DataflowWorkerLoggingFormatter().format(
            createLogRecord("test.message", null)));
  }

  @Test
  public void testWithMessageAndException() {
    MDC.setContextMap(ImmutableMap.<String, String>of(
        "dataflow.jobId", "testJobId",
        "dataflow.workerId", "testWorkerId",
        "dataflow.workId", "testWorkId"));
    assertEquals(
        "1970-01-01T00:00:00.001Z INFO testJobId testWorkerId testWorkId 2 LoggerName "
        + "test.message" + System.lineSeparator()
        + "java.lang.Throwable: exception.test.message" + System.lineSeparator()
        + "\tat declaringClass1.method1(file1.java:1)" + System.lineSeparator()
        + "\tat declaringClass2.method2(file2.java:1)" + System.lineSeparator()
        + "\tat declaringClass3.method3(file3.java:1)" + System.lineSeparator(),
        new DataflowWorkerLoggingFormatter().format(
            createLogRecord("test.message", createThrowable())));
  }

  @Test
  public void testWithException() {
    MDC.setContextMap(ImmutableMap.<String, String>of(
        "dataflow.jobId", "testJobId",
        "dataflow.workerId", "testWorkerId",
        "dataflow.workId", "testWorkId"));
    assertEquals(
        "1970-01-01T00:00:00.001Z INFO testJobId testWorkerId testWorkId 2 LoggerName null"
        + System.lineSeparator()
        + "java.lang.Throwable: exception.test.message" + System.lineSeparator()
        + "\tat declaringClass1.method1(file1.java:1)" + System.lineSeparator()
        + "\tat declaringClass2.method2(file2.java:1)" + System.lineSeparator()
        + "\tat declaringClass3.method3(file3.java:1)" + System.lineSeparator(),
        new DataflowWorkerLoggingFormatter().format(
            createLogRecord(null, createThrowable())));
  }

  @Test
  public void testWithoutExceptionOrMessage() {
    MDC.setContextMap(ImmutableMap.<String, String>of(
        "dataflow.jobId", "testJobId",
        "dataflow.workerId", "testWorkerId",
        "dataflow.workId", "testWorkId"));
    assertEquals(
        "1970-01-01T00:00:00.001Z INFO testJobId testWorkerId testWorkId 2 LoggerName null"
        + System.lineSeparator(),
        new DataflowWorkerLoggingFormatter().format(
            createLogRecord(null, null)));
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
