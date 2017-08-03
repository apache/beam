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
package org.apache.beam.fn.harness.stream;

import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;

/**
 * {@link #inbound(Iterator)} treats multiple {@link ByteString}s as a single input stream and
 * {@link #outbound(CloseableThrowingConsumer)} treats a single {@link OutputStream} as mulitple
 * {@link ByteString}s.
 */
public class DataStreams {
  /**
   * Converts multiple {@link ByteString}s into a single {@link InputStream}.
   *
   * <p>The iterator is accessed lazily. The supplied {@link Iterator} should block until
   * either it knows that no more values will be provided or it has the next {@link ByteString}.
   */
  public static InputStream inbound(Iterator<ByteString> bytes) {
    return new Inbound(bytes);
  }

  /**
   * Converts a single {@link OutputStream} into multiple {@link ByteString}s.
   */
  public static OutputStream outbound(CloseableThrowingConsumer<ByteString> consumer) {
    // TODO: Migrate logic from BeamFnDataBufferingOutboundObserver
    throw new UnsupportedOperationException();
  }

  /**
   * An input stream which concatenates multiple {@link ByteString}s. Lazily accesses the
   * first {@link Iterator} on first access of this input stream.
   *
   * <p>Closing this input stream has no effect.
   */
  private static class Inbound<T> extends InputStream {
    private static final InputStream EMPTY_STREAM = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    private final Iterator<ByteString> bytes;
    private InputStream currentStream;

    public Inbound(Iterator<ByteString> bytes) {
      this.currentStream = EMPTY_STREAM;
      this.bytes = bytes;
    }

    @Override
    public int read() throws IOException {
      int rval = -1;
      // Move on to the next stream if we have read nothing
      while ((rval = currentStream.read()) == -1 && bytes.hasNext()) {
        currentStream = bytes.next().newInput();
      }
      return rval;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int remainingLen = len;
      while ((remainingLen -= ByteStreams.read(
          currentStream, b, off + len - remainingLen, remainingLen)) > 0) {
        if (bytes.hasNext()) {
          currentStream = bytes.next().newInput();
        } else {
          int bytesRead = len - remainingLen;
          return bytesRead > 0 ? bytesRead : -1;
        }
      }
      return len - remainingLen;
    }
  }

  /**
   * Allows for one or more writing threads to append values to this iterator while one reading
   * thread reads values. {@link #hasNext()} and {@link #next()} will block until a value is
   * available or this has been closed.
   *
   * <p>External synchronization must be provided if multiple readers would like to access the
   * {@link Iterator#hasNext()} and {@link Iterator#next()} methods.
   *
   * <p>The order or values which are appended to this iterator is nondeterministic when multiple
   * threads call {@link #accept(Object)}.
   */
  public static class BlockingQueueIterator<T> implements
      CloseableThrowingConsumer<T>, Iterator<T> {
    private static final Object POISION_PILL = new Object();
    private final BlockingQueue<T> queue;

    /** Only accessed by {@link Iterator#hasNext()} and {@link Iterator#next()} methods. */
    private T currentElement;

    public BlockingQueueIterator(BlockingQueue<T> queue) {
      this.queue = queue;
    }

    @Override
    public void close() throws Exception {
      queue.put((T) POISION_PILL);
    }

    @Override
    public void accept(T t) throws Exception {
      queue.put(t);
    }

    @Override
    public boolean hasNext() {
      if (currentElement == null) {
        try {
          currentElement = queue.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      return currentElement != POISION_PILL;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T rval = currentElement;
      currentElement = null;
      return rval;
    }
  }
}
