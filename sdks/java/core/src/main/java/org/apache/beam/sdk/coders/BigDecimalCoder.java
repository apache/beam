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

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A {@link BigDecimalCoder} encodes {@link BigDecimal} in an integer and
 * a byte array. The integer represents the scale and the byte array
 * represents a {@link BigInteger}. The integer is in 4 bytes, big-endian.
 */
public class BigDecimalCoder extends AtomicCoder<BigDecimal> {

  @JsonCreator
  public static BigDecimalCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final BigDecimalCoder INSTANCE = new BigDecimalCoder();

  private BigDecimalCoder() {}

  @Override
  public void encode(BigDecimal value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null BigDecimal");
    }

    byte[] bigIntBytes = value.unscaledValue().toByteArray();

    DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeInt(value.scale());
    dataOutputStream.writeInt(bigIntBytes.length);
    dataOutputStream.write(bigIntBytes);
  }

  @Override
  public BigDecimal decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    DataInputStream dataInputStream = new DataInputStream(inStream);
    int scale = dataInputStream.readInt();
    int bigIntBytesSize = dataInputStream.readInt();

    byte[] bigIntBytes = new byte[bigIntBytesSize];
    dataInputStream.readFully(bigIntBytes);

    BigInteger bigInteger = new BigInteger(bigIntBytes);
    BigDecimal bigDecimal = new BigDecimal(bigInteger, scale);

    return bigDecimal;
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
  public boolean isRegisterByteSizeObserverCheap(BigDecimal value, Context context) {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code 8} plus the size of the {@link BigInteger} bytes.
   */
  @Override
  protected long getEncodedElementByteSize(BigDecimal value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null BigDecimal");
    }
    return 8 + value.unscaledValue().toByteArray().length;
  }
}
