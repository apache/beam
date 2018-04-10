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
package org.apache.beam.sdk.fn.stream;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.coders.Coder;

/**
 * {@link #inbound(Iterator)} treats multiple {@link ByteString}s as a single input stream and
 * {@link #outbound(OutputChunkConsumer)} treats a single {@link OutputStream} as multiple
 * {@link ByteString}s.
 */
// TODO: Migrate logic from BeamFnDataBufferingOutboundObserver to support Outbound
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
   * Converts a single {@link OutputStream} into multiple {@link ByteString ByteStrings}.
   */
  public static OutputStream outbound(OutputChunkConsumer<ByteString> consumer) {
    // TODO: Migrate logic from BeamFnDataBufferingOutboundObserver
    throw new UnsupportedOperationException();
  }

  /**
   * Reads chunks of output.
   *
   * @deprecated Used as a temporary placeholder until implementation of
   * {@link #outbound(OutputChunkConsumer)}.
   */
  @Deprecated
  public interface OutputChunkConsumer<T> {
    void read(T chunk) throws Exception;
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
   * An adapter which converts an {@link InputStream} to an {@link Iterator} of {@code T} values
   * using the specified {@link Coder}.
   *
   * <p>Note that this adapter follows the Beam Fn API specification for forcing values that decode
   * consuming zero bytes to consuming exactly one byte.
   *
   * <p>Note that access to the underlying {@link InputStream} is lazy and will only be invoked on
   * first access to {@link #next()} or {@link #hasNext()}.
   */
  public static class DataStreamDecoder<T> implements Iterator<T> {

    private enum State { READ_REQUIRED, HAS_NEXT, EOF }

    private final CountingInputStream countingInputStream;
    private final PushbackInputStream pushbackInputStream;
    private final Coder<T> coder;
    private State currentState;
    private T next;
    public DataStreamDecoder(Coder<T> coder, InputStream inputStream) {
      this.currentState = State.READ_REQUIRED;
      this.coder = coder;
      this.pushbackInputStream = new PushbackInputStream(inputStream, 1);
      this.countingInputStream = new CountingInputStream(pushbackInputStream);
    }

    @Override
    public boolean hasNext() {
      switch (currentState) {
        case EOF:
          return false;
        case READ_REQUIRED:
          try {
            int nextByte = pushbackInputStream.read();
            if (nextByte == -1) {
              currentState = State.EOF;
              return false;
            }

            pushbackInputStream.unread(nextByte);
            long count = countingInputStream.getCount();
            next = coder.decode(countingInputStream);
            // Skip one byte if decoding the value consumed 0 bytes.
            if (countingInputStream.getCount() - count == 0) {
              checkState(countingInputStream.read() != -1, "Unexpected EOF reached");
            }
            currentState = State.HAS_NEXT;
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
          return true;
        case HAS_NEXT:
          return true;
      }
      throw new IllegalStateException(String.format("Unknown state %s", currentState));
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      currentState = State.READ_REQUIRED;
      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
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
  public static class BlockingQueueIterator<T>
      implements AutoCloseable, Iterator<T> {
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

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
