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
import java.math.BigInteger;

/**
 * A {@link BigIntegerCoder} encodes a {@link BigInteger} as a byte array containing the big endian
 * two's-complement representation, encoded via {@link ByteArrayCoder}.
 */
public class BigIntegerCoder extends AtomicCoder<BigInteger> {

  public static BigIntegerCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final BigIntegerCoder INSTANCE = new BigIntegerCoder();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

  private BigIntegerCoder() {}

  @Override
  public void encode(BigInteger value, OutputStream outStream) throws IOException, CoderException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(BigInteger value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    checkNotNull(value, String.format("cannot encode a null %s", BigInteger.class.getSimpleName()));
    BYTE_ARRAY_CODER.encode(value.toByteArray(), outStream, context);
  }

  @Override
  public BigInteger decode(InputStream inStream) throws IOException, CoderException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public BigInteger decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    return new BigInteger(BYTE_ARRAY_CODER.decode(inStream, context));
  }

  @Override
  public void verifyDeterministic() {
    BYTE_ARRAY_CODER.verifyDeterministic();
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}. This coder is injective.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}, because {@link #getEncodedElementByteSize} runs in constant time.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(BigInteger value) {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return the size of the encoding as a byte array according to {@link ByteArrayCoder}
   */
  @Override
  protected long getEncodedElementByteSize(BigInteger value) throws Exception {
    checkNotNull(value, String.format("cannot encode a null %s", BigInteger.class.getSimpleName()));
    return BYTE_ARRAY_CODER.getEncodedElementByteSize(value.toByteArray());
  }
}
