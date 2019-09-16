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
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.ReadableDuration;

/**
 * A {@link Coder} that encodes a joda {@link Duration} as a {@link Long} using the format of {@link
 * VarLongCoder}.
 */
public class DurationCoder extends AtomicCoder<ReadableDuration> {

  public static DurationCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final DurationCoder INSTANCE = new DurationCoder();
  private static final TypeDescriptor<ReadableDuration> TYPE_DESCRIPTOR =
      new TypeDescriptor<ReadableDuration>() {};

  private static final VarLongCoder LONG_CODER = VarLongCoder.of();

  private DurationCoder() {}

  private Long toLong(ReadableDuration value) {
    return value.getMillis();
  }

  private ReadableDuration fromLong(Long decoded) {
    return Duration.millis(decoded);
  }

  @Override
  public void encode(ReadableDuration value, OutputStream outStream)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null ReadableDuration");
    }
    LONG_CODER.encode(toLong(value), outStream);
  }

  @Override
  public ReadableDuration decode(InputStream inStream) throws CoderException, IOException {
    return fromLong(LONG_CODER.decode(inStream));
  }

  @Override
  public void verifyDeterministic() {
    LONG_CODER.verifyDeterministic();
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
   * @return {@code true}, because it is cheap to ascertain the byte size of a long.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(ReadableDuration value) {
    return LONG_CODER.isRegisterByteSizeObserverCheap(toLong(value));
  }

  @Override
  public void registerByteSizeObserver(ReadableDuration value, ElementByteSizeObserver observer)
      throws Exception {
    LONG_CODER.registerByteSizeObserver(toLong(value), observer);
  }

  @Override
  public TypeDescriptor<ReadableDuration> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
