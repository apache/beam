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
package org.apache.beam.sdk.extensions.protobuf;

import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} for {@link ByteString} objects based on their encoded Protocol Buffer form.
 *
 * <p>When this code is used in a nested {@link Coder.Context}, the serialized {@link ByteString}
 * objects are first delimited by their size.
 */
public class ByteStringCoder extends CustomCoder<ByteString> {

  public static ByteStringCoder of() {
    return INSTANCE;
  }

  /***************************/

  private static final ByteStringCoder INSTANCE = new ByteStringCoder();
  private static final TypeDescriptor<ByteString> TYPE_DESCRIPTOR =
      new TypeDescriptor<ByteString>() {};

  private ByteStringCoder() {}

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
  public ByteString decode(InputStream inStream, Context context) throws IOException {
    if (context.isWholeStream) {
      return ByteString.readFrom(inStream);
    }

    int size = VarInt.decodeInt(inStream);
    // ByteString reads to the end of the input stream, so give it a limited stream of exactly
    // the right length. Also set its chunk size so that the ByteString will contain exactly
    // one chunk.
    return ByteString.readFrom(ByteStreams.limit(inStream, size), size);
  }

  @Override
  protected long getEncodedElementByteSize(ByteString value, Context context) throws Exception {
    int size = value.size();

    if (context.isWholeStream) {
      return size;
    }
    return VarInt.getLength(size) + size;
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
  public boolean isRegisterByteSizeObserverCheap(ByteString value, Context context) {
    return true;
  }

  @Override
  public TypeDescriptor<ByteString> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
