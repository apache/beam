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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Provides an efficient encoding for {@link Iterable}s containing small values by buffering up to
 * {@code bufferSize} bytes of data before prefixing the count. Note that each element needs to be
 * encoded in a nested context. See {@link Context Coder.Context} for more details.
 *
 * <p>To use this stream:
 *
 * <pre><code>
 * BufferedElementCountingOutputStream os = ...
 * for (Element E : elements) {
 *   os.markElementStart();
 *   // write an element to os
 * }
 * os.finish();
 * </code></pre>
 *
 * <p>The resulting output stream is:
 *
 * <pre>
 * countA element(0) element(1) ... element(countA - 1)
 * countB element(0) element(1) ... element(countB - 1)
 * ...
 * countX element(0) element(1) ... element(countX - 1)
 * countY
 * </pre>
 *
 * <p>To read this stream:
 *
 * <pre>{@code
 * InputStream is = ...
 * long count;
 * do {
 *   count = VarInt.decodeLong(is);
 *   for (int i = 0; i < count; ++i) {
 *     // read an element from is
 *   }
 * } while(count > 0);
 * }</pre>
 *
 * <p>The counts are encoded as variable length longs. See {@link VarInt#encode(long, OutputStream)}
 * for more details. The end of the iterable is detected by reading a count of 0.
 */
@Internal
@NotThreadSafe
public class BufferedElementCountingOutputStream extends OutputStream {
  private static final int MAX_POOLED = 12;

  @VisibleForTesting
  static final ArrayBlockingQueue<ByteBuffer> BUFFER_POOL = new ArrayBlockingQueue<>(MAX_POOLED);

  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private final ByteBuffer buffer;
  private final OutputStream os;
  private final long terminatorValue;
  private boolean finished;
  private long count;

  /**
   * Creates an output stream which encodes the number of elements output to it in a streaming
   * manner.
   */
  public BufferedElementCountingOutputStream(OutputStream os) {
    this(os, DEFAULT_BUFFER_SIZE, 0);
  }

  public BufferedElementCountingOutputStream(OutputStream os, long terminatorValue) {
    this(os, DEFAULT_BUFFER_SIZE, terminatorValue);
  }

  /**
   * Creates an output stream which encodes the number of elements output to it in a streaming
   * manner with the given {@code bufferSize}.
   */
  BufferedElementCountingOutputStream(OutputStream os, int bufferSize, long terminatorValue) {
    this.os = os;
    this.terminatorValue = terminatorValue;
    this.finished = false;
    this.count = 0;
    ByteBuffer buffer = BUFFER_POOL.poll();
    if (buffer == null) {
      buffer = ByteBuffer.allocate(bufferSize);
    }
    this.buffer = buffer;
  }

  /** Finishes the encoding by flushing any buffered data, and outputting a final count of 0. */
  public void finish() throws IOException {
    if (finished) {
      return;
    }
    flush();
    // Finish the stream with the terminatorValue.
    VarInt.encode(terminatorValue, os);
    if (!BUFFER_POOL.offer(buffer)) {
      // The pool is full, we can't store the buffer. We just drop the buffer.
    }
    finished = true;
  }

  /**
   * Marks that a new element is being output. This allows this output stream to use the buffer if
   * it had previously overflowed marking the start of a new block of elements.
   */
  public void markElementStart() throws IOException {
    if (finished) {
      throw new IOException("Stream has been finished. Can not add any more elements.");
    }
    count++;
  }

  @Override
  public void write(int b) throws IOException {
    if (finished) {
      throw new IOException("Stream has been finished. Can not write any more data.");
    }
    if (count == 0) {
      os.write(b);
      return;
    }

    if (buffer.hasRemaining()) {
      buffer.put((byte) b);
    } else {
      outputBuffer();
      os.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (finished) {
      throw new IOException("Stream has been finished. Can not write any more data.");
    }
    if (count == 0) {
      os.write(b, off, len);
      return;
    }

    if (buffer.remaining() >= len) {
      buffer.put(b, off, len);
    } else {
      outputBuffer();
      os.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    if (finished) {
      return;
    }
    outputBuffer();
    os.flush();
  }

  @Override
  public void close() throws IOException {
    finish();
    os.close();
  }

  // Output the buffer if it contains any data.
  private void outputBuffer() throws IOException {
    if (count > 0) {
      VarInt.encode(count, os);
      // We are using a heap based buffer and not a direct buffer so it is safe to access
      // the underlying array.
      os.write(buffer.array(), buffer.arrayOffset(), buffer.position());
      buffer.clear();
      // The buffer has been flushed so we must write to the underlying stream until
      // we learn of the next element. We reset the count to zero marking that we should
      // not use the buffer.
      count = 0;
    }
  }
}
