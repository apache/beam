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

import com.google.common.base.Utf8;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} that encodes {@link String Strings} in UTF-8 encoding.
 * If in a nested context, prefixes the string with an integer length field,
 * encoded via a {@link VarIntCoder}.
 */
public class StringUtf8Coder extends CustomCoder<String> {

  public static StringUtf8Coder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final StringUtf8Coder INSTANCE = new StringUtf8Coder();
  private static final TypeDescriptor<String> TYPE_DESCRIPTOR = new TypeDescriptor<String>() {};

  private static void writeString(String value, DataOutputStream dos)
      throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    VarInt.encode(bytes.length, dos);
    dos.write(bytes);
  }

  private static String readString(DataInputStream dis) throws IOException {
    int len = VarInt.decodeInt(dis);
    if (len < 0) {
      throw new CoderException("Invalid encoded string length: " + len);
    }
    byte[] bytes = new byte[len];
    dis.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private StringUtf8Coder() {}

  @Override
  public void encode(String value, OutputStream outStream, Context context)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    }
    if (context.isWholeStream) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (outStream instanceof ExposedByteArrayOutputStream) {
        ((ExposedByteArrayOutputStream) outStream).writeAndOwn(bytes);
      } else {
        outStream.write(bytes);
      }
    } else {
      writeString(value, new DataOutputStream(outStream));
    }
  }

  @Override
  public String decode(InputStream inStream, Context context)
      throws IOException {
    if (context.isWholeStream) {
      byte[] bytes = StreamUtils.getBytes(inStream);
      return new String(bytes, StandardCharsets.UTF_8);
    } else {
      try {
        return readString(new DataInputStream(inStream));
      } catch (EOFException | UTFDataFormatException exn) {
        // These exceptions correspond to decoding problems, so change
        // what kind of exception they're branded as.
        throw new CoderException(exn);
      }
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

  @Override
  public TypeDescriptor<String> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  /**
   * {@inheritDoc}
   *
   * @return the byte size of the UTF-8 encoding of the a string or, in a nested context,
   * the byte size of the encoding plus the encoded length prefix.
   */
  @Override
  public long getEncodedElementByteSize(String value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    }
    if (context.isWholeStream) {
      return Utf8.encodedLength(value);
    } else {
      try (CountingOutputStream countingStream =
          new CountingOutputStream(ByteStreams.nullOutputStream())) {
        DataOutputStream stream = new DataOutputStream(countingStream);
        writeString(value, stream);
        return countingStream.getCount();
      }
    }
  }
}
