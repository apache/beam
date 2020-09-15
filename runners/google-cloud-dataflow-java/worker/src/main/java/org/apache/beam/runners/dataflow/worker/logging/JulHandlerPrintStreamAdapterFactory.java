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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderMalfunctionError;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Formatter;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;

/**
 * A {@link PrintStream} factory that creates {@link PrintStream}s which output to the specified JUL
 * {@link Handler} at the specified {@link Level}.
 */
class JulHandlerPrintStreamAdapterFactory {
  private static final AtomicBoolean outputWarning = new AtomicBoolean(false);

  private static class JulHandlerPrintStream extends PrintStream {
    private static final String LOGGING_DISCLAIMER =
        String.format(
            "Please use a logger instead of System.out or System.err.%n"
                + "Please switch to using org.slf4j.Logger.%n"
                + "See: https://cloud.google.com/dataflow/pipelines/logging");
    // This limits the number of bytes which we buffer in case we don't have a flush.
    private static final int BUFFER_LIMIT = 1 << 10; // 1024 chars

    /** Hold reference of named logger to check configured {@link Level}. */
    private final Logger logger;

    private final Handler handler;
    private final String loggerName;
    private final StringBuilder buffer;
    private final Level messageLevel;
    private final CharsetDecoder decoder;
    private final CharBuffer decoded;
    private int carryOverBytes;
    private byte[] carryOverByteArray;

    private JulHandlerPrintStream(Handler handler, String loggerName, Level logLevel)
        throws UnsupportedEncodingException {
      super(
          new OutputStream() {
            @Override
            public void write(int i) throws IOException {
              throw new RuntimeException("All methods should be overwritten so this is unused");
            }
          },
          false,
          Charsets.UTF_8.name());
      this.handler = handler;
      this.loggerName = loggerName;
      this.messageLevel = logLevel;
      this.logger = Logger.getLogger(loggerName);
      this.buffer = new StringBuilder();
      this.decoder =
          Charset.defaultCharset()
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPLACE)
              .onUnmappableCharacter(CodingErrorAction.REPLACE);
      this.carryOverByteArray = new byte[6];
      this.carryOverBytes = 0;
      this.decoded = CharBuffer.allocate(BUFFER_LIMIT);
    }

    @Override
    public void flush() {
      publishIfNonEmpty(flushToString());
    }

    private synchronized String flushToString() {
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
      ByteBuffer incoming = ByteBuffer.wrap(a, offset, length);
      // Consume the added bytes, flushing on decoded newlines or if we hit
      // the buffer limit.
      String msg = null;
      synchronized (decoder) {
        decoded.clear();
        boolean flush = false;
        try {
          // Process any remaining bytes from last time by adding a byte at a time.
          while (carryOverBytes > 0 && incoming.hasRemaining()) {
            carryOverByteArray[carryOverBytes++] = incoming.get();
            ByteBuffer wrapped = ByteBuffer.wrap(carryOverByteArray, 0, carryOverBytes);
            decoder.decode(wrapped, decoded, false);
            if (!wrapped.hasRemaining()) {
              carryOverBytes = 0;
            }
          }
          carryOverBytes = 0;
          if (incoming.hasRemaining()) {
            CoderResult result = decoder.decode(incoming, decoded, false);
            if (result.isOverflow()) {
              flush = true;
            }
            // Keep the unread bytes.
            assert (incoming.remaining() <= carryOverByteArray.length);
            while (incoming.hasRemaining()) {
              carryOverByteArray[carryOverBytes++] = incoming.get();
            }
          }
        } catch (CoderMalfunctionError error) {
          decoder.reset();
          carryOverBytes = 0;
          error.printStackTrace();
        }
        decoded.flip();
        synchronized (this) {
          int startLength = buffer.length();
          buffer.append(decoded);
          if (flush || buffer.indexOf("\n", startLength) >= 0) {
            msg = flushToString();
          }
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
        msg = flushToString();
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
        msg = flushToString();
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
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(char c) {
      String msg;
      synchronized (this) {
        buffer.append(c);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(int i) {
      String msg;
      synchronized (this) {
        buffer.append(i);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(long l) {
      String msg;
      synchronized (this) {
        buffer.append(l);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(float f) {
      String msg;
      synchronized (this) {
        buffer.append(f);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(double d) {
      String msg;
      synchronized (this) {
        buffer.append(d);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(char[] a) {
      String msg;
      synchronized (this) {
        buffer.append(a);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(String s) {
      String msg;
      synchronized (this) {
        buffer.append(s);
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
    }

    @Override
    public void println(Object o) {
      String msg;
      synchronized (this) {
        buffer.append(o);
        msg = flushToString();
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
        msg = flushToString();
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
        msg = flushToString();
      }
      publishIfNonEmpty(msg);
      return this;
    }

    // Note to avoid a deadlock, publish may never be called synchronized. See BEAM-9399.
    private void publishIfNonEmpty(String message) {
      checkState(
          !Thread.holdsLock(this),
          "BEAM-9399: publish should not be called with the lock as it may cause deadlock");
      if (message == null || message.isEmpty()) {
        return;
      }
      if (logger.isLoggable(messageLevel)) {
        if (outputWarning.compareAndSet(false, true)) {
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
  static PrintStream create(Handler handler, String loggerName, Level messageLevel) {
    try {
      return new JulHandlerPrintStream(handler, loggerName, messageLevel);
    } catch (UnsupportedEncodingException exc) {
      throw new RuntimeException("Encoding not supported: " + Charsets.UTF_8.name(), exc);
    }
  }

  @VisibleForTesting
  static void reset() {
    outputWarning.set(false);
  }
}
