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
import org.joda.time.Instant;

/**
 * A {@link Coder} for joda {@link Instant} that encodes it as a big endian {@link Long} shifted
 * such that lexicographic ordering of the bytes corresponds to chronological order.
 */
public class InstantCoder extends AtomicCoder<Instant> {
  public static InstantCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final InstantCoder INSTANCE = new InstantCoder();
  private static final TypeDescriptor<Instant> TYPE_DESCRIPTOR = new TypeDescriptor<Instant>() {};

  private InstantCoder() {}

  @Override
  public void encode(Instant value, OutputStream outStream) throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }

    // Converts {@link Instant} to a {@code long} representing its millis-since-epoch,
    // but shifted so that the byte representation of negative values are lexicographically
    // ordered before the byte representation of positive values.
    //
    // This deliberately utilizes the well-defined underflow for {@code long} values.
    // See http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.18.2
    long shiftedMillis = value.getMillis() - Long.MIN_VALUE;
    new DataOutputStream(outStream).writeLong(shiftedMillis);
  }

  @Override
  public Instant decode(InputStream inStream) throws CoderException, IOException {
    long shiftedMillis;
    try {
      shiftedMillis = new DataInputStream(inStream).readLong();
    } catch (EOFException | UTFDataFormatException exn) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(exn);
    }

    // Produces an {@link Instant} from a {@code long} representing its millis-since-epoch,
    // but shifted so that the byte representation of negative values are lexicographically
    // ordered before the byte representation of positive values.
    //
    // This deliberately utilizes the well-defined overflow for {@code long} values.
    // See http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.18.2
    return new Instant(shiftedMillis + Long.MIN_VALUE);
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
   * @return {@code true}. The byte size for a big endian long is a constant.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Instant value) {
    return true;
  }

  @Override
  protected long getEncodedElementByteSize(Instant value) throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    return 8;
  }

  @Override
  public TypeDescriptor<Instant> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
