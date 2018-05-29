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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link BigEndianIntegerCoder} encodes {@link Integer Integers} in 4 bytes, big-endian.
 */
public class BigEndianIntegerCoder extends AtomicCoder<Integer> {

  public static BigEndianIntegerCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final BigEndianIntegerCoder INSTANCE = new BigEndianIntegerCoder();
  private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR = new TypeDescriptor<Integer>() {};

  private BigEndianIntegerCoder() {}

  @Override
  public void encode(Integer value, OutputStream outStream) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Integer");
    }
    new DataOutputStream(outStream).writeInt(value);
  }

  @Override
  public Integer decode(InputStream inStream)
      throws IOException, CoderException {
    try {
      return new DataInputStream(inStream).readInt();
    } catch (EOFException | UTFDataFormatException exn) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(exn);
    }
  }

  @Override
  public void verifyDeterministic() {}

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
  public boolean isRegisterByteSizeObserverCheap(Integer value) {
    return true;
  }

  @Override
  public TypeDescriptor<Integer> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code 4}, the size in bytes of an integer's big endian encoding.
   */
  @Override
  protected long getEncodedElementByteSize(Integer value)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Integer");
    }
    return 4;
  }
}
