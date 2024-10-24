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
package org.apache.beam.runners.fnexecution.wire;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.UnsafeByteOperations;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * A duplicate of {@link ByteStringCoder} that uses the Apache Beam vendored protobuf.
 *
 * <p>For internal use only, no backwards-compatibility guarantees.
 */
@Internal
public class ByteStringCoder extends AtomicCoder<ByteString> {

  public static ByteStringCoder of() {
    return INSTANCE;
  }

  /** ************************ */
  private static final ByteStringCoder INSTANCE = new ByteStringCoder();

  private static final TypeDescriptor<ByteString> TYPE_DESCRIPTOR =
      new TypeDescriptor<ByteString>() {};

  private ByteStringCoder() {}

  @Override
  public void encode(ByteString value, OutputStream outStream) throws IOException, CoderException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(ByteString value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null ByteString");
    }

    if (!context.isWholeStream) {
      // ByteString is not delimited, so write its size before its contents.
      VarInt.encode(value.size(), outStream);
    }
    value.writeTo(outStream);
  }

  @Override
  public ByteString decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public ByteString decode(InputStream inStream, Context context) throws IOException {
    if (context.isWholeStream) {
      return ByteString.readFrom(inStream);
    }

    int size = VarInt.decodeInt(inStream);
    if (size == 0) {
      return ByteString.EMPTY;
    }

    // we pre-allocate a byte[] and read into it, then wrap it with a ByteString rather than using
    // ByteString.readFrom since we know the length.  Doing so is significantly more efficient
    // because we don't need an intermediate buffer list.
    byte[] buf = new byte[size];
    ByteStreams.readFully(inStream, buf, 0, size);
    return UnsafeByteOperations.unsafeWrap(buf, 0, size);
  }

  @Override
  protected long getEncodedElementByteSize(ByteString value) throws Exception {
    int size = value.size();
    return (long) VarInt.getLength(size) + size;
  }

  @Override
  public void verifyDeterministic() {}

  /**
   * {@inheritDoc}
   *
   * <p>Returns true; the encoded output of two invocations of {@link ByteStringCoder} in the same
   * {@link Coder.Context} will be identical if and only if the original {@link ByteString} objects
   * are equal according to {@link Object#equals}.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns true. {@link ByteString#size} returns the size of an array and a {@link VarInt}.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(ByteString value) {
    return true;
  }

  @Override
  public TypeDescriptor<ByteString> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
