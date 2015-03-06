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

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Formats {@link LogRecord} into the following format:
 * ISO8601Date LogLevel JobId WorkerId WorkId ThreadId LoggerName LogMessage
 * with one or more additional lines for any {@link Throwable} associated with
 * the {@link LogRecord}. The exception is output using
 * {@link Throwable#printStackTrace()}.
 */
public class DataflowWorkerLoggingFormatter extends Formatter {
  private static final DateTimeFormatter DATE_FORMATTER =
      ISODateTimeFormat.dateTime().withZoneUTC();

  private static final InheritableThreadLocal<String> jobId = new InheritableThreadLocal<>();
  private static final InheritableThreadLocal<String> workerId = new InheritableThreadLocal<>();
  private static final InheritableThreadLocal<String> workId = new InheritableThreadLocal<>();

  /**
   * Sets the Job ID of the current thread, which will be inherited by child threads.
   */
  public static void setJobId(String newJobId) {
    jobId.set(newJobId);
  }

  /**
   * Sets the Worker ID of the current thread, which will be inherited by child threads.
   */
  public static void setWorkerId(String newWorkerId) {
    workerId.set(newWorkerId);
  }

  /**
   * Sets the Work ID of the current thread, which will be inherited by child threads.
   */
  public static void setWorkId(String newWorkId) {
    workId.set(newWorkId);
  }

  /**
   * Gets the Job ID of the current thread.
   */
  public static String getJobId() {
    return jobId.get();
  }

  /**
   * Gets the Worker ID of the current thread.
   */
  public static String getWorkerId() {
    return workerId.get();
  }

  /**
   * Gets the Work ID of the current thread.
   */
  public static String getWorkId() {
    return workId.get();
  }

  @Override
  public String format(LogRecord record) {
    String exception = formatException(record.getThrown());
    return DATE_FORMATTER.print(record.getMillis())
        + " " + MoreObjects.firstNonNull(LEVELS.get(record.getLevel()),
                                         record.getLevel().getName())
        + " " + record.getMessage()
        + " [" + MoreObjects.firstNonNull(jobId.get(), "unknown")
        + " " + MoreObjects.firstNonNull(workerId.get(), "unknown")
        + " " + MoreObjects.firstNonNull(workId.get(), "unknown")
        + " " + record.getThreadID()
        + "] " + record.getLoggerName() + System.lineSeparator()
        + (exception != null ? exception : "");
  }

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
}
