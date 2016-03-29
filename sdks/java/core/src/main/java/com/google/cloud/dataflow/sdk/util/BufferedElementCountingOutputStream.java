/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder.Context;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an efficient encoding for {@link Iterable}s containing small values by
 * buffering up to {@code bufferSize} bytes of data before prefixing the count.
 * Note that each element needs to be encoded in a nested context. See
 * {@link Context Coder.Context} for more details.
 *
 * <p>To use this stream:
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
 * <pre>
 * countA element(0) element(1) ... element(countA - 1)
 * countB element(0) element(1) ... element(countB - 1)
 * ...
 * countX element(0) element(1) ... element(countX - 1)
 * countY
 * </pre>
 *
 * <p>To read this stream:
 * <pre><code>
 * InputStream is = ...
 * long count;
 * do {
 *   count = VarInt.decodeLong(is);
 *   for (int i = 0; i < count; ++i) {
 *     // read an element from is
 *   }
 * } while(count > 0);
 * </code></pre>
 *
 * <p>The counts are encoded as variable length longs. See {@link VarInt#encode(long, OutputStream)}
 * for more details. The end of the iterable is detected by reading a count of 0.
 */
@NotThreadSafe
public class BufferedElementCountingOutputStream extends OutputStream {
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private final ByteBuffer buffer;
  private final OutputStream os;
  private boolean finished;
  private long count;

  /**
   * Creates an output stream which encodes the number of elements output to it in a streaming
   * manner.
   */
  public BufferedElementCountingOutputStream(OutputStream os) {
    this(os, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Creates an output stream which encodes the number of elements output to it in a streaming
   * manner with the given {@code bufferSize}.
   */
  BufferedElementCountingOutputStream(OutputStream os, int bufferSize) {
    this.buffer = ByteBuffer.allocate(bufferSize);
    this.os = os;
    this.finished = false;
    this.count = 0;
  }

  /**
   * Finishes the encoding by flushing any buffered data,
   * and outputting a final count of 0.
   */
  public void finish() throws IOException {
    if (finished) {
      return;
    }
    flush();
    // Finish the stream by stating that there are 0 elements that follow.
    VarInt.encode(0, os);
    finished = true;
  }

  /**
   * Marks that a new element is being output. This allows this output stream
   * to use the buffer if it had previously overflowed marking the start of a new
   * block of elements.
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

