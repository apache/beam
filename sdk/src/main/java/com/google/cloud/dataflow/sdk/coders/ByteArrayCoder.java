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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A ByteArrayCoder encodes byte[] objects.
 *
 * If in a nested context, prefixes the encoded array with a VarInt encoding
 * of the length.
 */
@SuppressWarnings("serial")
public class ByteArrayCoder extends AtomicCoder<byte[]> {
  @JsonCreator
  public static ByteArrayCoder of() {
    return INSTANCE;
  }


  /////////////////////////////////////////////////////////////////////////////

  private static final ByteArrayCoder INSTANCE = new ByteArrayCoder();

  private ByteArrayCoder() {}

  @Override
  public void encode(byte[] value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null byte[]");
    }
    if (!context.isWholeStream) {
      VarInt.encode(value.length, outStream);
    }
    outStream.write(value);
  }

  @Override
  public byte[] decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    if (context.isWholeStream) {
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      ByteStreams.copy(inStream, outStream);
      return outStream.toByteArray();
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
  public boolean isDeterministic() {
    return true;
  }

  /**
   * Returns true since registerByteSizeObserver() runs in constant time.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(byte[] value, Context context) {
    return true;
  }

  @Override
  protected long getEncodedElementByteSize(byte[] value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null byte[]");
    }
    long size = 0;
    if (!context.isWholeStream) {
      size += VarInt.getLength(value.length);
    }
    return size + value.length;
  }
}
