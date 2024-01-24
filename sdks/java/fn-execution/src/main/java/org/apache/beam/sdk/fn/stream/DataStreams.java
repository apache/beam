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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.WeightedList;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * {@link DataStreamDecoder} treats multiple {@link ByteString}s as a single input stream decoding
 * values with the supplied iterator. {@link #outbound(OutputChunkConsumer)} treats a single {@link
 * OutputStream} as multiple {@link ByteString}s.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DataStreams {
  public static final int DEFAULT_OUTBOUND_BUFFER_LIMIT_BYTES = 1_000_000;

  /** The number of bytes to add to cache estimates for each element in the weighted list. */
  private static final long BYTES_LIST_ELEMENT_OVERHEAD = 8L;

  /**
   * Converts a single element delimited {@link OutputStream} into multiple {@link ByteString
   * ByteStrings}.
   *
   * <p>Note that users must call {@link ElementDelimitedOutputStream#delimitElement} after each
   * element.
   *
   * <p>Note that this {@link OutputStream} follows the Beam Fn API specification for forcing values
   * that encode producing zero bytes to produce exactly one byte.
   */
  public static ElementDelimitedOutputStream outbound(OutputChunkConsumer<ByteString> consumer) {
    return outbound(consumer, DEFAULT_OUTBOUND_BUFFER_LIMIT_BYTES);
  }

  /**
   * Converts a single element delimited {@link OutputStream} into multiple {@link ByteString
   * ByteStrings} using the specified maximum chunk size.
   *
   * <p>Note that users must call {@link ElementDelimitedOutputStream#delimitElement} after each
   * element.
   *
   * <p>Note that this {@link OutputStream} follows the Beam Fn API specification for forcing values
   * that encode producing zero bytes to produce exactly one byte.
   */
  public static ElementDelimitedOutputStream outbound(
      OutputChunkConsumer<ByteString> consumer, int maximumChunkSize) {
    return new ElementDelimitedOutputStream(consumer, maximumChunkSize);
  }

  /**
   * An adapter which wraps an {@link OutputChunkConsumer} as an {@link OutputStream}.
   *
   * <p>Note that this adapter follows the Beam Fn API specification for forcing values that encode
   * producing zero bytes to produce exactly one byte.
   *
   * <p>Note that users must invoke {@link #delimitElement} at each element boundary.
   */
  public static final class ElementDelimitedOutputStream extends OutputStream {
    private final OutputChunkConsumer<ByteString> consumer;
    private final ByteStringOutputStream output;
    private final int maximumChunkSize;
    int previousPosition;

    public ElementDelimitedOutputStream(
        OutputChunkConsumer<ByteString> consumer, int maximumChunkSize) {
      this.consumer = consumer;
      this.maximumChunkSize = maximumChunkSize;
      this.output = new ByteStringOutputStream(maximumChunkSize);
    }

    public void delimitElement() throws IOException {
      // If the previous encoding was exactly zero bytes, output a single marker byte as per
      // https://s.apache.org/beam-fn-api-send-and-receive-data
      if (previousPosition == output.size()) {
        write(0);
      }
      previousPosition = output.size();
    }

    @Override
    public void write(int i) throws IOException {
      output.write(i);
      if (maximumChunkSize == output.size()) {
        internalFlush();
      }
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
      int spaceRemaining = maximumChunkSize - output.size();
      // Fill the first partially filled buffer.
      if (length > spaceRemaining) {
        output.write(b, offset, spaceRemaining);
        offset += spaceRemaining;
        length -= spaceRemaining;
        internalFlush();
      }
      // Fill buffers completely.
      while (length > maximumChunkSize) {
        output.write(b, offset, maximumChunkSize);
        offset += maximumChunkSize;
        length -= maximumChunkSize;
        internalFlush();
      }
      // Fill any remainder.
      output.write(b, offset, length);
    }

    @Override
    public void close() throws IOException {
      if (output.size() > 0) {
        consumer.read(output.toByteString());
      }
      output.close();
    }

    /** Can only be called if at least one byte has been written. */
    private void internalFlush() throws IOException {
      consumer.read(output.toByteStringAndReset());
      // Set the previous position to an invalid position representing that a previous buffer
      // was written to.
      previousPosition = -1;
    }
  }

  /**
   * A callback which is invoked whenever the {@link #outbound} {@link OutputStream} becomes full.
   *
   * <p>{@link #outbound(OutputChunkConsumer)}.
   */
  public interface OutputChunkConsumer<T> {
    void read(T chunk) throws IOException;
  }

  /**
   * An adapter which converts an {@link InputStream} to a {@link PrefetchableIterator} of {@code T}
   * values using the specified {@link Coder}.
   *
   * <p>Note that this adapter follows the Beam Fn API specification for forcing values that decode
   * consuming zero bytes to consuming exactly one byte.
   *
   * <p>Note that access to the underlying {@link InputStream} is lazy and will only be invoked on
   * first access to {@link #next}, {@link #hasNext}, {@link #isReady}, and {@link #prefetch}.
   *
   * <p>Note that {@link #isReady} and {@link #prefetch} rely on non-empty {@link ByteString}s being
   * returned via the underlying {@link PrefetchableIterator} otherwise the {@link #prefetch} will
   * seemingly make zero progress yet will actually advance through the empty pages.
   */
  public static class DataStreamDecoder<T> implements PrefetchableIterator<T> {
    private final PrefetchableIterator<ByteString> inputByteStrings;
    private final Inbound inbound;
    private final Coder<T> coder;

    public DataStreamDecoder(Coder<T> coder, PrefetchableIterator<ByteString> inputStream) {
      this.coder = coder;
      this.inputByteStrings = inputStream;
      this.inbound = new Inbound();
    }

    /**
     * Skips any remaining bytes in the current {@link ByteString} moving to the next {@link
     * ByteString} in the underlying {@link ByteString} {@link Iterator iterator} and decoding
     * elements till at the next boundary.
     */
    public WeightedList<T> decodeFromChunkBoundaryToChunkBoundary() {
      ByteString byteString = inputByteStrings.next();
      inbound.currentStream = byteString.newInput();
      inbound.position = 0;

      try {
        InputStream previousStream = inbound.currentStream;
        List<T> rvals = new ArrayList<>();
        while (previousStream == inbound.currentStream && inbound.currentStream.available() != 0) {
          T next = next();
          rvals.add(next);
        }

        // Uses the size of the ByteString as an approximation for the heap size occupied by the
        // page, considering an overhead of {@link BYTES_LIST_ELEMENT_OVERHEAD} for each element.
        long elementOverhead = rvals.size() * BYTES_LIST_ELEMENT_OVERHEAD;
        long totalWeight = byteString.size() + elementOverhead;

        return new WeightedList<>(rvals, totalWeight);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public boolean isReady() {
      try {
        return inbound.isReady();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void prefetch() {
      if (!isReady()) {
        inputByteStrings.prefetch();
      }
    }

    @Override
    public boolean hasNext() {
      try {
        return !inbound.isEof();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      try {
        long previousPosition = inbound.position;
        InputStream previousStream = inbound.currentStream;
        T next = coder.decode(inbound);
        // Skip one byte if decoding the value consumed 0 bytes.
        if (previousPosition == inbound.position && previousStream == inbound.currentStream) {
          checkState(inbound.read() != -1, "Unexpected EOF reached");
        }
        return next;
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private static final InputStream EMPTY_STREAM = ByteString.EMPTY.newInput();

    /**
     * An input stream which concatenates multiple {@link ByteString}s. Lazily accesses the {@link
     * Iterator} on first access of this input stream.
     *
     * <p>Closing this input stream has no effect.
     */
    private class Inbound extends InputStream {
      private int position; // Position within the current input stream.
      private InputStream currentStream;

      public Inbound() {
        this.currentStream = EMPTY_STREAM;
      }

      public boolean isReady() throws IOException {
        // Note that ByteString#newInput is guaranteed to return the length of the entire ByteString
        // minus the number of bytes that have been read so far and can be reliably used to tell
        // us whether we are at the end of the stream.
        while (currentStream.available() == 0) {
          if (!inputByteStrings.isReady()) {
            return false;
          }
          if (!inputByteStrings.hasNext()) {
            return true;
          }
          currentStream = inputByteStrings.next().newInput();
          position = 0;
        }
        return true;
      }

      public boolean isEof() throws IOException {
        // Note that ByteString#newInput is guaranteed to return the length of the entire ByteString
        // minus the number of bytes that have been read so far and can be reliably used to tell
        // us whether we are at the end of the stream.
        while (currentStream.available() == 0) {
          if (!inputByteStrings.hasNext()) {
            return true;
          }
          currentStream = inputByteStrings.next().newInput();
          position = 0;
        }
        return false;
      }

      @Override
      public int read() throws IOException {
        int read;
        // Move on to the next stream if this stream is done
        while ((read = currentStream.read()) == -1) {
          if (!inputByteStrings.hasNext()) {
            return -1;
          }
          currentStream = inputByteStrings.next().newInput();
          position = 0;
        }
        position += 1;
        return read;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        int remainingLen = len;
        while (remainingLen > 0) {
          int read;
          // Move on to the next stream if this stream is done. Note that ByteString.newInput
          // guarantees that read will consume the entire ByteString if the passed in length is
          // greater than or equal to the remaining amount.
          while ((read = currentStream.read(b, off + len - remainingLen, remainingLen)) == -1) {
            if (!inputByteStrings.hasNext()) {
              int bytesRead = len - remainingLen;
              position += bytesRead;
              return bytesRead > 0 ? bytesRead : -1;
            }
            currentStream = inputByteStrings.next().newInput();
            position = 0;
          }
          remainingLen -= read;
        }
        position += len;
        return len;
      }
    }
  }
}
