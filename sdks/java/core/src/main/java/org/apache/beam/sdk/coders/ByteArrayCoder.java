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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * A {@link Coder} for {@code byte[]}.
 *
 * <p>The encoding format is as follows:
 *
 * <ul>
 *   <li>If in a non-nested context (the {@code byte[]} is the only value in the stream), the bytes
 *       are read/written directly.
 *   <li>If in a nested context, the bytes are prefixed with the length of the array, encoded via a
 *       {@link VarIntCoder}.
 * </ul>
 */
public class ByteArrayCoder extends AtomicCoder<byte[]> {

  public static ByteArrayCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final ByteArrayCoder INSTANCE = new ByteArrayCoder();
  private static final TypeDescriptor<byte[]> TYPE_DESCRIPTOR = new TypeDescriptor<byte[]>() {};

  private ByteArrayCoder() {}

  @Override
  public void encode(byte[] value, OutputStream outStream) throws IOException, CoderException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(byte[] value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null byte[]");
    }
    if (!context.isWholeStream) {
      VarInt.encode(value.length, outStream);
      outStream.write(value);
    } else {
      outStream.write(value);
    }
  }

  /**
   * Encodes the provided {@code value} with the identical encoding to {@link #encode}, but with
   * optimizations that take ownership of the value.
   *
   * <p>Once passed to this method, {@code value} should never be observed or mutated again.
   */
  public void encodeAndOwn(byte[] value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (!context.isWholeStream) {
      VarInt.encode(value.length, outStream);
      outStream.write(value);
    } else {
      if (outStream instanceof ExposedByteArrayOutputStream) {
        ((ExposedByteArrayOutputStream) outStream).writeAndOwn(value);
      } else {
        outStream.write(value);
      }
    }
  }

  @Override
  public byte[] decode(InputStream inStream) throws IOException, CoderException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public byte[] decode(InputStream inStream, Context context) throws IOException, CoderException {
    if (context.isWholeStream) {
      return StreamUtils.getBytesWithoutClosing(inStream);
    } else {
      int length = VarInt.decodeInt(inStream);
      if (length < 0) {
        throw new IOException("invalid length " + length);
      }
      byte[] value = new byte[length];
      ByteStreams.readFully(inStream, value);
      return value;
    }
  }

  @Override
  public void verifyDeterministic() {}

  /**
   * {@inheritDoc}
   *
   * @return objects that are equal if the two arrays contain the same bytes.
   */
  @Override
  public Object structuralValue(byte[] value) {
    return new StructuralByteArray(value);
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} since {@link #getEncodedElementByteSize} runs in constant time using the
   *     {@code length} of the provided array.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(byte[] value) {
    return true;
  }

  @Override
  public TypeDescriptor<byte[]> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  protected long getEncodedElementByteSize(byte[] value) throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null byte[]");
    }
    return (long) VarInt.getLength(value.length) + value.length;
  }
}
