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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A {@link PrintStream} factory that creates {@link PrintStream}s which output to the specified JUL
 * {@link Handler} at the specified {@link Level}.
 */
class JulHandlerPrintStreamAdapterFactory {
  private static final AtomicBoolean outputWarning = new AtomicBoolean(false);

  /**
   * Creates a {@link PrintStream} which redirects all output to the JUL {@link Handler} with the
   * specified {@code loggerName} and {@code level}.
   */
  static PrintStream create(Handler handler, String loggerName, Level messageLevel) {
    try {
      return new PrintStream(
          new JulHandlerAdapterOutputStream(handler, loggerName, messageLevel),
          false,
          StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * An output stream adapter which is able to take a stream of UTF-8 data and output to a named JUL
   * log handler. The log messages will be buffered until the system dependent new line separator is
   * seen, at which point the buffered string will be output.
   */
  private static class JulHandlerAdapterOutputStream extends OutputStream {
    private static final String LOGGING_DISCLAIMER =
        String.format(
            "Please use a logger instead of System.out or System.err.%n"
                + "Please switch to using org.slf4j.Logger.%n"
                + "See: https://cloud.google.com/dataflow/pipelines/logging");
    // This limits the number of bytes which we buffer in case we don't see a newline character.
    private static final int BUFFER_LIMIT = 1 << 14; // 16384 bytes
    private static final byte[] NEW_LINE = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    /** Hold reference of named logger to check configured {@link Level}. */
    private Logger logger;

    private Handler handler;
    private String loggerName;
    private ByteArrayOutputStream baos;
    private Level messageLevel;
    private int matched = 0;

    private JulHandlerAdapterOutputStream(Handler handler, String loggerName, Level logLevel) {
      this.handler = handler;
      this.loggerName = loggerName;
      this.messageLevel = logLevel;
      this.logger = Logger.getLogger(loggerName);
      this.baos = new ByteArrayOutputStream(BUFFER_LIMIT);
    }

    @Override
    public void write(int b) {
      if (outputWarning.compareAndSet(false, true)) {
        publish(Level.WARNING, LOGGING_DISCLAIMER);
      }

      baos.write(b);
      // Check to see if the next byte matches further into new line string.
      if (NEW_LINE[matched] == b) {
        matched += 1;
        // If we have matched the entire new line, output the contents of the buffer.
        if (matched == NEW_LINE.length) {
          output();
        }
      } else {
        // Reset the match
        matched = 0;
      }
      if (baos.size() == BUFFER_LIMIT) {
        output();
      }
    }

    @Override
    public void flush() throws IOException {
      output();
    }

    @Override
    public void close() throws IOException {
      output();
    }

    private void output() {
      // If nothing was output, do not log anything
      if (baos.size() == 0) {
        return;
      }
      try {
        String message = baos.toString(StandardCharsets.UTF_8.name());
        // Strip the new line if it exists
        if (message.endsWith(System.lineSeparator())) {
          message = message.substring(0, message.length() - System.lineSeparator().length());
        }

        publish(messageLevel, message);
      } catch (UnsupportedEncodingException e) {
        publish(
            Level.SEVERE, String.format("Unable to decode string output to stdout/stderr %s", e));
      }
      matched = 0;
      baos.reset();
    }

    private void publish(Level level, String message) {
      if (logger.isLoggable(level)) {
        LogRecord log = new LogRecord(level, message);
        log.setLoggerName(loggerName);
        handler.publish(log);
      }
    }
  }

  @VisibleForTesting
  static void reset() {
    outputWarning.set(false);
  }
}
