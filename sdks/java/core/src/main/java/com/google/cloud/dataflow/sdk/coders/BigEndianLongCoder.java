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

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

/**
 * A {@link BigEndianLongCoder} encodes {@link Long}s in 8 bytes, big-endian.
 */
public class BigEndianLongCoder extends AtomicCoder<Long> {

  @JsonCreator
  public static BigEndianLongCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final BigEndianLongCoder INSTANCE = new BigEndianLongCoder();

  private BigEndianLongCoder() {}

  @Override
  public void encode(Long value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null Long");
    }
    new DataOutputStream(outStream).writeLong(value);
  }

  @Override
  public Long decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    try {
      return new DataInputStream(inStream).readLong();
    } catch (EOFException | UTFDataFormatException exn) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(exn);
    }
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
   * @return {@code true}, since {@link #getEncodedElementByteSize} returns a constant.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Long value, Context context) {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code 8}, the byte size of a big-endian encoded {@code Long}.
   */
  @Override
  protected long getEncodedElementByteSize(Long value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Long");
    }
    return 8;
  }
}
