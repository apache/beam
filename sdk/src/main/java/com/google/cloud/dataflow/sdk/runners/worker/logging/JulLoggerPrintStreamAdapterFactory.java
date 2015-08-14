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

import com.google.common.base.Throwables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link PrintStream} factory that creates {@link PrintStream}s which output
 * to the named JUL {@link Logger} at the specified {@link Level}.
 */
class JulLoggerPrintStreamAdapterFactory {
  private static final AtomicBoolean outputWarning = new AtomicBoolean(false);

  /**
   * Creates a {@link PrintStream} which redirects all output to a JUL {@link Logger}
   * with the given {@code name} at the specified {@code level}.
   */
  static PrintStream create(String name, Level level) {
    try {
      return new PrintStream(
          new JulLoggerAdapterOutputStream(name, level),
          false, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * An output stream adapter which is able to take a stream of UTF-8 data and output
   * to a named JUL logger. The log messages will be buffered until the system
   * dependent new line separator is seen, at which point the buffered string will be
   * output.
   */
  private static class JulLoggerAdapterOutputStream extends OutputStream {
    private static final String LOGGING_DISCLAIMER = String.format(
        "Please use a logger instead of System.out or System.err.%n"
        + "Please switch to using org.slf4j.Logger.%n"
        + "See: https://cloud.google.com/dataflow/pipelines/logging");
    // This limits the number of bytes which we buffer in case we don't see a newline character.
    private static final int BUFFER_LIMIT = 1 << 14; // 16384 bytes
    private static final byte[] NEW_LINE = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
    private Logger logger;
    private ByteArrayOutputStream baos;
    private Level logLevel;
    private int matched = 0;

    private JulLoggerAdapterOutputStream(String name, Level logLevel) {
      this.logger = Logger.getLogger(name);
      this.logLevel = logLevel;
      this.baos = new ByteArrayOutputStream(BUFFER_LIMIT);
    }

    @Override
    public void write(int b) {
      if (outputWarning.compareAndSet(false, true)) {
        logger.warning(LOGGING_DISCLAIMER);
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
        logger.log(logLevel, message);
      } catch (UnsupportedEncodingException e) {
        logger.severe(String.format("Unable to decode string output to stdout/stderr %s", e));
      }
      matched = 0;
      baos.reset();
    }
  }
}
