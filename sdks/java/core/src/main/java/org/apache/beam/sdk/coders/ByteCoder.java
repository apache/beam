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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link ByteCoder} encodes {@link Byte} values in 1 byte using Java serialization. */
public class ByteCoder extends AtomicCoder<Byte> {

  public static ByteCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final ByteCoder INSTANCE = new ByteCoder();
  private static final TypeDescriptor<Byte> TYPE_DESCRIPTOR = new TypeDescriptor<Byte>() {};

  private ByteCoder() {}

  @Override
  public void encode(Byte value, OutputStream outStream) throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null Byte");
    }
    outStream.write(value);
  }

  @Override
  public Byte decode(InputStream inStream) throws IOException, CoderException {
    try {
      // value will be between 0-255, -1 for EOF
      int value = inStream.read();
      if (value == -1) {
        throw new EOFException("EOF encountered decoding 1 byte from input stream");
      }
      return (byte) value;
    } catch (EOFException | UTFDataFormatException exn) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(exn);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@link ByteCoder} will never throw a {@link Coder.NonDeterministicException}; bytes can
   * always be encoded deterministically.
   */
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
   * @return {@code true}. {@link ByteCoder#getEncodedElementByteSize} returns a constant.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Byte value) {
    return true;
  }

  @Override
  public TypeDescriptor<Byte> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code 1}, the byte size of a {@link Byte} encoded using Java serialization.
   */
  @Override
  protected long getEncodedElementByteSize(Byte value) throws Exception {
    if (value == null) {
      throw new CoderException("cannot estimate size for unsupported null value");
    }
    return 1;
  }
}
