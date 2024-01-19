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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderMalfunctionError;
import java.nio.charset.CodingErrorAction;
import java.util.Formatter;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A {@link PrintStream} factory that creates {@link PrintStream}s which output to the specified JUL
 * {@link Handler} at the specified {@link Level}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class JulHandlerPrintStreamAdapterFactory {
  private static final AtomicBoolean OUTPUT_WARNING = new AtomicBoolean(false);

  @VisibleForTesting
  static final String LOGGING_DISCLAIMER =
      String.format(
          "Please use a logger instead of System.out or System.err.%n"
              + "Please switch to using org.slf4j.Logger.%n"
              + "See: https://cloud.google.com/dataflow/pipelines/logging");

  private static class JulHandlerPrintStream extends PrintStream {
    // This limits the number of bytes which we buffer in case we don't have a flush.
    private static final int BUFFER_LIMIT = 1 << 10; // 1024 chars

    /** Hold reference of named logger to check configured {@link Level}. */
    private final Logger logger;

    private final Handler handler;
    private final String loggerName;
    private final Level messageLevel;

    @GuardedBy("this")
    private final StringBuilder buffer;

    @GuardedBy("this")
    private final CharsetDecoder decoder;

    @GuardedBy("this")
    private final CharBuffer decoded;

    @GuardedBy("this")
    private ByteArrayOutputStream carryOverBytes;

    private JulHandlerPrintStream(
        Handler handler, String loggerName, Level logLevel, Charset charset)
        throws UnsupportedEncodingException {
      super(
          new OutputStream() {
            @Override
            public void write(int i) throws IOException {
              throw new RuntimeException("All methods should be overwritten so this is unused");
            }
          },
          false,
          charset.name());
      this.handler = handler;
      this.loggerName = loggerName;
      this.messageLevel = logLevel;
      this.logger = Logger.getLogger(loggerName);
      this.buffer = new StringBuilder();
      this.decoder =
          charset
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPLACE)
              .onUnmappableCharacter(CodingErrorAction.REPLACE);
      this.carryOverBytes = new ByteArrayOutputStream();
      this.decoded = CharBuffer.allocate(BUFFER_LIMIT);
    }

    @Override
    public void flush() {
      publishIfNonEmpty(flushBufferToString());
    }

    private synchronized String flushBufferToString() {
      if (buffer.length() > 0 && buffer.charAt(buffer.length() - 1) == '\n') {
        buffer.setLength(buffer.length() - 1);
      }
      if (buffer.length() == 0) {
        return null;
      }
      String result = buffer.toString();
      buffer.setLength(0);
      return result;
    }

    @Override
    public void close() {
      flush();
    }

    @Override
    public boolean checkError() {
      return false;
    }

    @Override
    public synchronized void write(int i) {
      buffer.append(i);
    }

    @Override
    public void write(byte[] a, int offset, int length) {
      if (length == 0) {
        return;
      }

      ByteBuffer incoming = ByteBuffer.wrap(a, offset, length);
      assert incoming.hasArray();

      String msg = null;
      // Consume the added bytes, flushing on decoded newlines or if we hit
      // the buffer limit.
      synchronized (this) {
        int startLength = buffer.length();

        try {
          // Process any remaining bytes from last time by adding a byte at a time.
          while (carryOverBytes.size() > 0 && incoming.hasRemaining()) {
            carryOverBytes.write(incoming.get());
            ByteBuffer wrapped =
                ByteBuffer.wrap(carryOverBytes.toByteArray(), 0, carryOverBytes.size());
            decoder.decode(wrapped, decoded, false);
            if (!wrapped.hasRemaining()) {
              carryOverBytes.reset();
            }
          }

          // Append chunks while we are hitting the decoded buffer limit
          while (decoder.decode(incoming, decoded, false).isOverflow()) {
            decoded.flip();
            buffer.append(decoded);
            decoded.clear();
          }

          // Append the partial chunk
          decoded.flip();
          buffer.append(decoded);
          decoded.clear();

          // Check to see if we should output this message
          if (buffer.length() > BUFFER_LIMIT || buffer.indexOf("\n", startLength) >= 0) {
            msg = flushBufferToString();
          }

          // Keep all unread bytes.
          carryOverBytes.write(
              incoming.array(), incoming.arrayOffset() + incoming.position(), incoming.remaining());
        } catch (CoderMalfunctionError error) {
          decoder.reset();
          carryOverBytes.reset();
          error.printStackTrace();
        }
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public synchronized void print(boolean b) {
      buffer.append(b ? "true" : "false");
    }

    @Override
    public synchronized void print(char c) {
      buffer.append(c);
    }

    @Override
    public synchronized void print(int i) {
      buffer.append(i);
    }

    @Override
    public synchronized void print(long l) {
      buffer.append(l);
    }

    @Override
    public synchronized void print(float f) {
      buffer.append(f);
    }

    @Override
    public synchronized void print(double d) {
      buffer.append(d);
    }

    @Override
    public void print(char[] a) {
      boolean flush = false;
      for (char c : a) {
        if (c == '\n') {
          flush = true;
        }
      }
      String msg;
      synchronized (this) {
        buffer.append(a);
        if (!flush) {
          return;
        }
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void print(String s) {
      boolean flush = s.indexOf('\n') >= 0;
      String msg;
      synchronized (this) {
        buffer.append(s);
        if (!flush) {
          return;
        }
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void print(Object o) {
      print(o.toString());
    }

    @Override
    public void println() {
      flush();
    }

    @Override
    public void println(boolean b) {
      String msg;
      synchronized (this) {
        buffer.append(b);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(char c) {
      String msg;
      synchronized (this) {
        buffer.append(c);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(int i) {
      String msg;
      synchronized (this) {
        buffer.append(i);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(long l) {
      String msg;
      synchronized (this) {
        buffer.append(l);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(float f) {
      String msg;
      synchronized (this) {
        buffer.append(f);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(double d) {
      String msg;
      synchronized (this) {
        buffer.append(d);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(char[] a) {
      String msg;
      synchronized (this) {
        buffer.append(a);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(String s) {
      String msg;
      synchronized (this) {
        buffer.append(s);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(Object o) {
      String msg;
      synchronized (this) {
        buffer.append(o);
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public PrintStream format(String format, Object... args) {
      return format(Locale.getDefault(), format, args);
    }

    @Override
    public PrintStream format(Locale locale, String format, Object... args) {
      String msg;
      synchronized (this) {
        int startLength = buffer.length();
        Formatter formatter = new Formatter(buffer, locale);
        formatter.format(format, args);
        if (buffer.indexOf("\n", startLength) < 0) {
          return this;
        }
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
      return this;
    }

    @Override
    public PrintStream append(CharSequence cs, int start, int limit) {
      CharSequence subsequence = cs.subSequence(start, limit);
      boolean flush = false;
      for (int i = 0; i < subsequence.length(); ++i) {
        if (subsequence.charAt(i) == '\n') {
          flush = true;
          break;
        }
      }
      String msg;
      synchronized (this) {
        buffer.append(cs.subSequence(start, limit));
        if (!flush) {
          return this;
        }
        msg = flushBufferToString();
      }
      publishIfNonEmpty(msg);
      return this;
    }

    private void publishIfNonEmpty(String message) {
      if (message == null || message.isEmpty()) {
        return;
      }
      if (logger.isLoggable(messageLevel)) {
        if (OUTPUT_WARNING.compareAndSet(false, true)) {
          LogRecord log = new LogRecord(Level.WARNING, LOGGING_DISCLAIMER);
          log.setLoggerName(loggerName);
          handler.publish(log);
        }
        LogRecord log = new LogRecord(messageLevel, message);
        log.setLoggerName(loggerName);
        handler.publish(log);
      }
    }
  }

  /**
   * Creates a {@link PrintStream} which redirects all output to the JUL {@link Handler} with the
   * specified {@code loggerName} and {@code level}.
   */
  static PrintStream create(
      Handler handler, String loggerName, Level messageLevel, Charset charset) {
    try {
      return new JulHandlerPrintStream(handler, loggerName, messageLevel, charset);
    } catch (UnsupportedEncodingException exc) {
      throw new RuntimeException("Encoding not supported: " + charset.name(), exc);
    }
  }

  static void reset() {
    OUTPUT_WARNING.set(false);
  }
}
