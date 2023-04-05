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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;

/**
 * A {@link Coder} which is able to take any existing coder and wrap it such that it is only invoked
 * in the {@link org.apache.beam.sdk.coders.Coder.Context#OUTER outer context}. The data
 * representing the element is prefixed with a length using a variable integer encoding.
 *
 * @param <T> the type of the values being transcoded
 */
public class LengthPrefixCoder<T> extends StructuredCoder<T> {

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
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    valueCoder.encode(value, bos, Context.OUTER);
    VarInt.encode(bos.size(), outStream);
    bos.writeTo(outStream);
  }

  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
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
    if (valueCoder instanceof StructuredCoder) {
      // If valueCoder is a StructuredCoder then we can ask it directly for the encoded size of
      // the value, adding the number of bytes to represent the length.
      long valueSize = ((StructuredCoder<T>) valueCoder).getEncodedElementByteSize(value);
      return VarInt.getLength(valueSize) + valueSize;
    }

    // If value is not a StructuredCoder then fall back to the default StructuredCoder behavior
    // of encoding and counting the bytes. The encoding will include the length prefix.
    return super.getEncodedElementByteSize(value);
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
