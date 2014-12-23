/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.util.VarInt;
import com.google.common.io.ByteStreams;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.charset.Charset;

/**
 * A StringUtf8Coder encodes Java Strings in UTF-8 encoding.
 * If in a nested context, prefixes the string with a VarInt length field.
 */
@SuppressWarnings("serial")
public class StringUtf8Coder extends AtomicCoder<String> {

  @JsonCreator
  public static StringUtf8Coder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final StringUtf8Coder INSTANCE = new StringUtf8Coder();

  private static class Singletons {
    private static final Charset UTF8 = Charset.forName("UTF-8");
  }

  // Writes a string with VarInt size prefix, supporting large strings.
  private static void writeString(String value, DataOutputStream dos)
      throws IOException {
    byte[] bytes = value.getBytes(Singletons.UTF8);
    VarInt.encode(bytes.length, dos);
    dos.write(bytes);
  }

  // Reads a string with VarInt size prefix, supporting large strings.
  private static String readString(DataInputStream dis) throws IOException {
    int len = VarInt.decodeInt(dis);
    if (len < 0) {
      throw new CoderException("Invalid encoded string length: " + len);
    }
    byte[] bytes = new byte[len];
    dis.readFully(bytes);
    return new String(bytes, Singletons.UTF8);
  }

  private StringUtf8Coder() {}

  @Override
  public void encode(String value, OutputStream outStream, Context context)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    }
    if (context.isWholeStream) {
      outStream.write(value.getBytes(Singletons.UTF8));
    } else {
      writeString(value, new DataOutputStream(outStream));
    }
  }

  @Override
  public String decode(InputStream inStream, Context context)
      throws IOException {
    if (context.isWholeStream) {
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      ByteStreams.copy(inStream, outStream);
      // ByteArrayOutputStream.toString provides no Charset overloads.
      return outStream.toString("UTF-8");
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
  public boolean isDeterministic() {
    return true;
  }

  protected long getEncodedElementByteSize(String value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    }
    if (context.isWholeStream) {
      return value.getBytes(Singletons.UTF8).length;
    } else {
      DataOutputStream stream = new DataOutputStream(new ByteArrayOutputStream());
      writeString(value, stream);
      return stream.size();
    }
  }
}
