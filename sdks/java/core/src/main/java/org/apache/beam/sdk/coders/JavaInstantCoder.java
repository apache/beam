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
import java.time.Instant;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} for Java time {@link Instant} that encodes it as a big endian {@link Long}
 * shifted such that lexicographic ordering of the bytes corresponds to chronological order, and
 * also encodes a {@link Integer} for the nanoseconds-of-second adjustment.
 */
public class JavaInstantCoder extends AtomicCoder<Instant> {
  public static JavaInstantCoder of() {
    return INSTANCE;
  }

  private static final JavaInstantCoder INSTANCE = new JavaInstantCoder();
  private static final TypeDescriptor<Instant> TYPE_DESCRIPTOR = new TypeDescriptor<Instant>() {};

  private JavaInstantCoder() {}

  @Override
  public void encode(Instant value, OutputStream outStream) throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }

    // Converts {@link Instant} to a {@code long} representing its seconds-since-epoch, and to a
    // {@code int} representing its nanosecond-of-second. seconds-since-epoch is shifted so that
    // the byte representation of negative values are lexicographically ordered before the byte
    // representation of positive values.
    long shiftedSeconds = value.getEpochSecond() - Long.MIN_VALUE;
    int nanoOfSecond = value.getNano();

    DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeLong(shiftedSeconds);
    dataOutputStream.writeInt(nanoOfSecond);
  }

  @Override
  public Instant decode(InputStream inStream) throws CoderException, IOException {
    long shiftedSeconds;
    int nanoOfSecond;
    DataInputStream dataInputStream = new DataInputStream(inStream);
    try {
      shiftedSeconds = dataInputStream.readLong();
      nanoOfSecond = dataInputStream.readInt();
    } catch (EOFException | UTFDataFormatException exn) {
      throw new CoderException(exn);
    }

    return Instant.ofEpochSecond(shiftedSeconds + Long.MIN_VALUE, nanoOfSecond);
  }
}
