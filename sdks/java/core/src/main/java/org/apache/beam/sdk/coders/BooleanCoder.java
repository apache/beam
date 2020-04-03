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

/** A {@link Coder} for {@link Boolean}. */
public class BooleanCoder extends AtomicCoder<Boolean> {
  private static final ByteCoder BYTE_CODER = ByteCoder.of();

  private static final BooleanCoder INSTANCE = new BooleanCoder();

  /** Returns the singleton instance of {@link BooleanCoder}. */
  public static BooleanCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(Boolean value, OutputStream os) throws IOException {
    BYTE_CODER.encode(value ? (byte) 1 : 0, os);
  }

  @Override
  public Boolean decode(InputStream is) throws IOException {
    Byte value = BYTE_CODER.decode(is);
    if (value == 0) {
      return false;
    } else if (value == 1) {
      return true;
    }
    throw new IOException(String.format("Expected 0 or 1, got %d", value));
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(Boolean value) {
    return true;
  }

  @Override
  protected long getEncodedElementByteSize(Boolean value) throws Exception {
    return 1;
  }
}
