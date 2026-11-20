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
package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} which is able to take any existing coder and wrap it such that it is only invoked
 * in the {@link org.apache.beam.sdk.coders.Coder.Context#OUTER outer context}. The data
 * representing the element is prefixed with a length using a variable integer encoding.
 *
 * @param <T> the type of the values being transcoded
 */
public class LengthPrefixCoder<T> extends StructuredCoder<T> {

  private static final int MAX_POOLED_BUFFER_SIZE = 64 * 1024;
  private static final int MAX_POOL_DEPTH = 4;

  @SuppressWarnings("type.argument") // object guaranteed to be non-null
  private static final ThreadLocal<@NonNull BufferPool> BUFFER_POOL =
      ThreadLocal.withInitial(BufferPool::new);

  private static final class BufferPool {
    private @Nullable ReusableByteArrayOutputStream head;
    private int depth;

    ReusableByteArrayOutputStream acquire() {
      ReusableByteArrayOutputStream buf = head;
      if (buf != null) {
        head = buf.next;
        buf.next = null;
        depth--;
        return buf;
      }
      return new ReusableByteArrayOutputStream();
    }

    void release(ReusableByteArrayOutputStream buf) {
      if (buf.capacity() <= MAX_POOLED_BUFFER_SIZE && depth < MAX_POOL_DEPTH) {
        buf.resetFast();
        buf.next = head;
        head = buf;
        depth++;
      }
    }
  }

  private static final class ReusableByteArrayOutputStream extends OutputStream {
    private byte[] buf = new byte[256];
    private int count;
    @Nullable ReusableByteArrayOutputStream next;

    private void ensureCapacity(int minCapacity) {
      if (minCapacity - buf.length > 0) {
        int newCapacity = buf.length << 1;
        if (newCapacity - minCapacity < 0) {
          newCapacity = minCapacity;
        }
        if (newCapacity < 0) {
          if (minCapacity < 0) {
            throw new OutOfMemoryError();
          }
          newCapacity = Integer.MAX_VALUE - 8;
        }
        buf = Arrays.copyOf(buf, newCapacity);
      }
    }

    @Override
    public void write(int b) {
      ensureCapacity(count + 1);
      buf[count] = (byte) b;
      count += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      if (off < 0 || len < 0 || off > b.length - len) {
        throw new IndexOutOfBoundsException();
      }
      ensureCapacity(count + len);
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    int sizeFast() {
      return count;
    }

    int capacity() {
      return buf.length;
    }

    void writeToStream(OutputStream out) throws IOException {
      out.write(buf, 0, count);
    }

    void resetFast() {
      count = 0;
    }
  }

  public static <T> LengthPrefixCoder<T> of(Coder<T> valueCoder) {
    checkNotNull(valueCoder, "Coder not expected to be null");
    return new LengthPrefixCoder<>(valueCoder);
  }

  /////////////////////////////////////////////////////////////////////////////

  private final Coder<T> valueCoder;

  private LengthPrefixCoder(Coder<T> valueCoder) {
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    if (valueCoder instanceof ByteArrayCoder || valueCoder instanceof StringUtf8Coder) {
      valueCoder.encode(value, outStream, Context.NESTED);
      return;
    }
    BufferPool pool = BUFFER_POOL.get();
    ReusableByteArrayOutputStream bos = pool.acquire();
    try {
      valueCoder.encode(value, bos, Context.OUTER);
      VarInt.encode(bos.sizeFast(), outStream);
      bos.writeToStream(outStream);
    } finally {
      pool.release(bos);
    }
  }

  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    if (valueCoder instanceof ByteArrayCoder || valueCoder instanceof StringUtf8Coder) {
      return valueCoder.decode(inStream, Context.NESTED);
    }
    long size = VarInt.decodeLong(inStream);
    return valueCoder.decode(ByteStreams.limit(inStream, size), Context.OUTER);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(valueCoder);
  }

  /** Gets the value coder that will be prefixed by the length. */
  public Coder<T> getValueCoder() {
    return valueCoder;
  }

  /**
   * {@code LengthPrefixCoder} is deterministic if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    valueCoder.verifyDeterministic();
  }

  /**
   * {@code LengthPrefixCoder} is consistent with equals if the nested {@code Coder} is.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean consistentWithEquals() {
    return valueCoder.consistentWithEquals();
  }

  /**
   * Overridden to short-circuit the default {@code StructuredCoder} behavior of encoding and
   * counting the bytes. The size is known to be the size of the value plus the number of bytes
   * required to prefix the length.
   *
   * <p>{@inheritDoc}
   */
  @Override
  protected long getEncodedElementByteSize(T value) throws Exception {
    long valueSize = valueCoder.getEncodedElementByteSize(value);
    return VarInt.getLength(valueSize) + valueSize;
  }

  /**
   * {@code LengthPrefixCoder} is cheap if {@code valueCoder} is cheap.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(T value) {
    return valueCoder.isRegisterByteSizeObserverCheap(value);
  }
}
