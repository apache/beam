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
package org.apache.beam.runners.core.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;

/**
 * Coder for {@link BitSet} that stores an empty bit set as a byte array with a single 0 element. In
 * general BitSetCoder should be preferred as it encodes an empty bit set as an empty byte array.
 * However, there are cases where non-empty values are useful to indicate presence.
 */
public class SentinelBitSetCoder extends AtomicCoder<BitSet> {

  private static final SentinelBitSetCoder INSTANCE = new SentinelBitSetCoder();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

  private SentinelBitSetCoder() {}

  public static SentinelBitSetCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BitSet value, OutputStream outStream) throws CoderException, IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(BitSet value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null BitSet");
    }
    byte[] bytes = value.isEmpty() ? new byte[] {0} : value.toByteArray();
    BYTE_ARRAY_CODER.encodeAndOwn(bytes, outStream, context);
  }

  @Override
  public BitSet decode(InputStream inStream) throws CoderException, IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public BitSet decode(InputStream inStream, Context context) throws CoderException, IOException {
    return BitSet.valueOf(BYTE_ARRAY_CODER.decode(inStream, context));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(
        this,
        "SentinelBitSetCoder requires its ByteArrayCoder to be deterministic.",
        BYTE_ARRAY_CODER);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
