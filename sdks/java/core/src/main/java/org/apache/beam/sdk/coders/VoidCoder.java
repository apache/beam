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

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} for {@link Void}. Uses zero bytes per {@link Void}.
 */
public class VoidCoder extends AtomicCoder<Void> {

  @JsonCreator
  public static VoidCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final VoidCoder INSTANCE = new VoidCoder();
  private static final TypeDescriptor<Void> TYPE_DESCRIPTOR = new TypeDescriptor<Void>() {};

  private VoidCoder() {}

  @Override
  public void encode(Void value, OutputStream outStream, Context context) {
    // Nothing to write!
  }

  @Override
  public Void decode(InputStream inStream, Context context) {
    // Nothing to read!
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @return  {@code true}. {@link VoidCoder} is (vacuously) injective.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}. {@link VoidCoder#getEncodedElementByteSize} runs in constant time.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Void value, Context context) {
    return true;
  }

  @Override
  public TypeDescriptor<Void> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  protected long getEncodedElementByteSize(Void value, Context context)
      throws Exception {
    return 0;
  }
}
