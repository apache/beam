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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;

/** Coder for {@link BitSet}. */
public class BitSetCoder extends AtomicCoder<BitSet> {
  private static final BitSetCoder INSTANCE = new BitSetCoder();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

  private BitSetCoder() {}

  public static BitSetCoder of() {
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
    BYTE_ARRAY_CODER.encodeAndOwn(value.toByteArray(), outStream, context);
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
        this, "BitSetCoder requires its ByteArrayCoder to be deterministic.", BYTE_ARRAY_CODER);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
