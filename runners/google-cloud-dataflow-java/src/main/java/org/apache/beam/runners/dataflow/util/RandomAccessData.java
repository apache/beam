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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An elastic-sized byte array which allows you to manipulate it as a stream, or access it directly.
 * This allows for a quick succession of moving bytes from an {@link InputStream} to this wrapper to
 * be used as an {@link OutputStream} and vice versa. This wrapper also provides random access to
 * bytes stored within. This wrapper allows users to finely control the number of byte copies that
 * occur.
 *
 * <p>Anything stored within the in-memory buffer from offset {@link #size()} is considered
 * temporary unused storage.
 */
@NotThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RandomAccessData {
  /**
   * A {@link Coder} which encodes the valid parts of this stream. This follows the same encoding
   * scheme as {@link ByteArrayCoder}. This coder is deterministic and consistent with equals.
   *
   * <p>This coder does not support encoding positive infinity.
   */
  public static class RandomAccessDataCoder extends AtomicCoder<RandomAccessData> {
    private static final RandomAccessDataCoder INSTANCE = new RandomAccessDataCoder();

    public static RandomAccessDataCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(RandomAccessData value, OutputStream outStream)
        throws CoderException, IOException {
      encode(value, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(RandomAccessData value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      if (Objects.equals(value, POSITIVE_INFINITY)) {
        throw new CoderException("Positive infinity can not be encoded.");
      }
      if (!context.isWholeStream) {
        VarInt.encode(value.size, outStream);
      }
      value.writeTo(outStream, 0, value.size);
    }

    @Override
    public RandomAccessData decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public RandomAccessData decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      RandomAccessData rval = new RandomAccessData();
      if (!context.isWholeStream) {
        int length = VarInt.decodeInt(inStream);
        rval.readFrom(inStream, 0, length);
      } else {
        ByteStreams.copy(inStream, rval.asOutputStream());
      }
      return rval;
    }

    @Override
    public void verifyDeterministic() {}

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(RandomAccessData value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(RandomAccessData value) throws Exception {
      if (value == null) {
        throw new CoderException("cannot encode a null in memory stream");
      }
      return (long) VarInt.getLength(value.size) + value.size;
    }
  }

  public static final UnsignedLexicographicalComparator UNSIGNED_LEXICOGRAPHICAL_COMPARATOR =
      new UnsignedLexicographicalComparator();

  /**
   * A {@link Comparator} that compares two byte arrays lexicographically. It compares values as a
   * list of unsigned bytes. The first pair of values that follow any common prefix, or when one
   * array is a prefix of the other, treats the shorter array as the lesser. For example, {@code []
   * < [0x01] < [0x01, 0x7F] < [0x01, 0x80] < [0x02] < POSITIVE INFINITY}.
   *
   * <p>Note that a token type of positive infinity is supported and is greater than all other
   * {@link RandomAccessData}.
   */
  public static final class UnsignedLexicographicalComparator
      implements Comparator<RandomAccessData>, Serializable {
    // Do not instantiate
    private UnsignedLexicographicalComparator() {}

    @Override
    public int compare(RandomAccessData o1, RandomAccessData o2) {
      return compare(o1, o2, 0 /* start from the beginning */);
    }

    /** Compare the two sets of bytes starting at the given offset. */
    @SuppressWarnings("ReferenceEquality") // equals overload calls into this compare method
    public int compare(RandomAccessData o1, RandomAccessData o2, int startOffset) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == POSITIVE_INFINITY) {
        return 1;
      }
      if (o2 == POSITIVE_INFINITY) {
        return -1;
      }

      int minBytesLen = Math.min(o1.size, o2.size);
      for (int i = startOffset; i < minBytesLen; i++) {
        // unsigned comparison
        int b1 = o1.buffer[i] & 0xFF;
        int b2 = o2.buffer[i] & 0xFF;
        if (b1 == b2) {
          continue;
        }
        // Return the stream with the smaller byte as the smaller value.
        return b1 - b2;
      }
      // If one is a prefix of the other, return the shorter one as the smaller one.
      // If both lengths are equal, then both streams are equal.
      return o1.size - o2.size;
    }

    /** Compute the length of the common prefix of the two provided sets of bytes. */
    public int commonPrefixLength(RandomAccessData o1, RandomAccessData o2) {
      int minBytesLen = Math.min(o1.size, o2.size);
      for (int i = 0; i < minBytesLen; i++) {
        // unsigned comparison
        int b1 = o1.buffer[i] & 0xFF;
        int b2 = o2.buffer[i] & 0xFF;
        if (b1 != b2) {
          return i;
        }
      }
      return minBytesLen;
    }
  }

  /** A token type representing positive infinity. */
  static final RandomAccessData POSITIVE_INFINITY = new RandomAccessData(0);

  /**
   * Returns a RandomAccessData that is the smallest value of same length which is strictly greater
   * than this. Note that if this is empty or is all 0xFF then a token value of positive infinity is
   * returned.
   *
   * <p>The {@link UnsignedLexicographicalComparator} supports comparing {@link RandomAccessData}
   * with support for positive infinity.
   */
  public RandomAccessData increment() throws IOException {
    RandomAccessData copy = copy();
    for (int i = copy.size - 1; i >= 0; --i) {
      if (copy.buffer[i] != UnsignedBytes.MAX_VALUE) {
        copy.buffer[i] = UnsignedBytes.checkedCast(UnsignedBytes.toInt(copy.buffer[i]) + 1L);
        return copy;
      }
    }
    return POSITIVE_INFINITY;
  }

  private static final int DEFAULT_INITIAL_BUFFER_SIZE = 128;

  /** Constructs a RandomAccessData with a default buffer size. */
  public RandomAccessData() {
    this(DEFAULT_INITIAL_BUFFER_SIZE);
  }

  /** Constructs a RandomAccessData with the initial buffer. */
  public RandomAccessData(byte[] initialBuffer) {
    checkNotNull(initialBuffer);
    this.buffer = initialBuffer;
    this.size = initialBuffer.length;
  }

  /** Constructs a RandomAccessData with the given buffer size. */
  public RandomAccessData(int initialBufferSize) {
    checkArgument(initialBufferSize >= 0, "Expected initial buffer size to be greater than zero.");
    this.buffer = new byte[initialBufferSize];
  }

  private byte[] buffer;
  private int size;

  /** Returns the backing array. */
  public byte[] array() {
    return buffer;
  }

  /** Returns the number of bytes in the backing array that are valid. */
  public int size() {
    return size;
  }

  /** Resets the end of the stream to the specified position. */
  public void resetTo(int position) {
    ensureCapacity(position);
    size = position;
  }

  private final OutputStream outputStream =
      new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          ensureCapacity(size + 1);
          buffer[size] = (byte) b;
          size += 1;
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
          ensureCapacity(size + length);
          System.arraycopy(b, offset, buffer, size, length);
          size += length;
        }
      };

  /**
   * Returns an output stream which writes to the backing buffer from the current position. Note
   * that the internal buffer will grow as required to accommodate all data written.
   */
  public OutputStream asOutputStream() {
    return outputStream;
  }

  /**
   * Returns an {@link InputStream} wrapper which supplies the portion of this backing byte buffer
   * starting at {@code offset} and up to {@code length} bytes. Note that the returned {@link
   * InputStream} is only a wrapper and any modifications to the underlying {@link RandomAccessData}
   * will be visible by the {@link InputStream}.
   */
  public InputStream asInputStream(final int offset, final int length) {
    return new ByteArrayInputStream(buffer, offset, length);
  }

  /**
   * Writes {@code length} bytes starting at {@code offset} from the backing data store to the
   * specified output stream.
   */
  public void writeTo(OutputStream out, int offset, int length) throws IOException {
    out.write(buffer, offset, length);
  }

  /**
   * Reads {@code length} bytes from the specified input stream writing them into the backing data
   * store starting at {@code offset}.
   *
   * <p>Note that the in memory stream will be grown to ensure there is enough capacity.
   */
  public void readFrom(InputStream inStream, int offset, int length) throws IOException {
    ensureCapacity(offset + length);
    ByteStreams.readFully(inStream, buffer, offset, length);
    size = offset + length;
  }

  /** Returns a copy of this RandomAccessData. */
  public RandomAccessData copy() throws IOException {
    RandomAccessData copy = new RandomAccessData(size);
    writeTo(copy.asOutputStream(), 0, size);
    return copy;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof RandomAccessData)) {
      return false;
    }

    return UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(this, (RandomAccessData) other) == 0;
  }

  @Override
  public int hashCode() {
    int result = 1;
    for (int i = 0; i < size; ++i) {
      result = 31 * result + buffer[i];
    }

    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("buffer", Arrays.copyOf(buffer, size))
        .add("size", size)
        .toString();
  }

  private void ensureCapacity(int minCapacity) {
    // If we have enough space, don't grow the buffer.
    if (minCapacity <= buffer.length) {
      return;
    }

    // Try to double the size of the buffer, if that's not enough, just use the new capacity.
    // Note that we use Math.min(long, long) to not cause overflow on the multiplication.
    int newCapacity = (int) Math.min(Integer.MAX_VALUE - 8, buffer.length * 2L);
    if (newCapacity < minCapacity) {
      newCapacity = minCapacity;
    }
    buffer = Arrays.copyOf(buffer, newCapacity);
  }
}
