/*
 * Copyright (C) 2015 Google Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

/**
 * A {@link Coder} that encodes {@link Long Longs} using between 1 and 10 bytes. Negative
 * numbers always take 10 bytes, so {@link BigEndianLongCoder} may be preferable for
 * longs that are known to often be large or negative.
 */
public class VarLongCoder extends AtomicCoder<Long> {

  @JsonCreator
  public static VarLongCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final VarLongCoder INSTANCE = new VarLongCoder();

  private VarLongCoder() {}

  @Override
  public void encode(Long value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null Long");
    }
    VarInt.encode(value.longValue(), outStream);
  }

  @Override
  public Long decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    try {
      return VarInt.decodeLong(inStream);
    } catch (EOFException | UTFDataFormatException exn) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(exn);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}. {@link VarLongCoder} is injective.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}. {@link #getEncodedElementByteSize} is cheap.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Long value, Context context) {
    return true;
  }

  @Override
  protected long getEncodedElementByteSize(Long value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Long");
    }
    return VarInt.getLength(value.longValue());
  }
}
