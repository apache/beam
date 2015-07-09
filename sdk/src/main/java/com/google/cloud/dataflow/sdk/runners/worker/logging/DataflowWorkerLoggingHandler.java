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

import static com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingInitializer.LEVELS;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * Formats {@link LogRecord} into JSON format for Cloud Logging.
 * Any exception is represented using {@link Throwable#printStackTrace()}.
 */
public class DataflowWorkerLoggingHandler extends Handler {
  /**
   * Formats the throwable as per {@link Throwable#printStackTrace()}.
   *
   * @param thrown The throwable to format.
   * @return A string containing the contents of {@link Throwable#printStackTrace()}.
   */
  public static String formatException(Throwable thrown) {
    if (thrown == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    thrown.printStackTrace(pw);
    pw.close();
    return sw.toString();
  }

  // Null after close().
  private JsonGenerator generator;

  /**
   * Constructs a handler that writes to a file.
   */
  public DataflowWorkerLoggingHandler(String fileName) throws IOException {
    this(new BufferedOutputStream(new FileOutputStream(new File(fileName), true /* append */)));
  }

  /**
   * Constructs a handler that writes to an arbitrary output stream.
   */
  public DataflowWorkerLoggingHandler(OutputStream output) throws IOException {
    generator = new ObjectMapper().getFactory().createGenerator(output, JsonEncoding.UTF8);
  }

  @Override
  public synchronized void publish(LogRecord record) {
    if (!isLoggable(record)) {
      return;
    }
    try {
      // Generating a JSON map like:
      // {"timestamp": {"seconds": 1435835832, "nanos": 123456789}, ...  "message": "hello"}
      generator.writeStartObject();
      // Write the timestamp.
      generator.writeFieldName("timestamp");
      generator.writeStartObject();
      generator.writeNumberField("seconds", record.getMillis() / 1000);
      generator.writeNumberField("nanos", (record.getMillis() % 1000) * 1000000);
      generator.writeEndObject();
      // Write the severity.
      generator.writeObjectField(
          "severity",
          MoreObjects.firstNonNull(LEVELS.get(record.getLevel()), record.getLevel().getName()));
      // Write the other labels.
      writeIfNotNull("message", record.getMessage());
      writeIfNotNull("thread", String.valueOf(record.getThreadID()));
      writeIfNotNull("job", DataflowWorkerLoggingMDC.getJobId());
      writeIfNotNull("stage", DataflowWorkerLoggingMDC.getStageName());
      writeIfNotNull("step", DataflowWorkerLoggingMDC.getStepName());
      writeIfNotNull("worker", DataflowWorkerLoggingMDC.getWorkerId());
      writeIfNotNull("work", DataflowWorkerLoggingMDC.getWorkId());
      writeIfNotNull("logger", record.getLoggerName());
      writeIfNotNull("exception", formatException(record.getThrown()));
      generator.writeEndObject();
      generator.writeRaw(System.lineSeparator());
    } catch (IOException e) {
      if (getErrorManager() != null) {
        getErrorManager().error("Unable to publish", e, ErrorManager.WRITE_FAILURE);
      }
    }
    // This implementation is based on that of java.util.logging.FileHandler, which flushes in a
    // synchronized context like this. Unfortunately the maximum throughput for generating log
    // entries will be the inverse of the flush latency. That could be as little as one hundred
    // log entries per second on some systems. For higher throughput this should be changed to
    // batch publish operations while writes and flushes are in flight on a different thread.
    flush();
  }

  /**
   * Check if a LogRecord will be logged.
   * <p>
   * This method checks if the <tt>LogRecord</tt> has an appropriate level and
   * whether it satisfies any <tt>Filter</tt>.  It will also return false if
   * the handler has been closed, or the LogRecord is null.
   * <p>
   */
  @Override
  public boolean isLoggable(LogRecord record) {
    return generator != null && record != null && super.isLoggable(record);
  }

  /**
   * Appends a JSON key/value pair if the specified val is not null.
   */
  private void writeIfNotNull(String name, String val) throws IOException {
    if (val != null) {
      generator.writeStringField(name, val);
    }
  }

  @Override
  public synchronized void flush() {
    try {
      if (generator != null) {
        generator.flush();
      }
    } catch (IOException e) {
      if (getErrorManager() != null) {
        getErrorManager().error("Unable to flush", e, ErrorManager.FLUSH_FAILURE);
      }
    }
  }

  @Override
  public synchronized void close() {
    // Flush any in-flight content, though there should not actually be any because
    // the generator is currently flushed in the synchronized publish() method.
    flush();
    // Close the generator and log file.
    try {
      if (generator != null) {
        generator.close();
      }
    } catch (IOException e) {
      if (getErrorManager() != null) {
        getErrorManager().error("Unable to close", e, ErrorManager.CLOSE_FAILURE);
      }
    }
    generator = null;
  }
}
