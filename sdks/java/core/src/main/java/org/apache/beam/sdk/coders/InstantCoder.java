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

import com.google.common.base.Converter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

/**
 * A {@link Coder} for joda {@link Instant} that encodes it as a big endian {@link Long}
 * shifted such that lexicographic ordering of the bytes corresponds to chronological order.
 */
public class InstantCoder extends AtomicCoder<Instant> {
  public static InstantCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final InstantCoder INSTANCE = new InstantCoder();
  private static final TypeDescriptor<Instant> TYPE_DESCRIPTOR = new TypeDescriptor<Instant>() {};

  private static final BigEndianLongCoder LONG_CODER = BigEndianLongCoder.of();

  private InstantCoder() {}

  private static final Converter<Instant, Long> ORDER_PRESERVING_CONVERTER =
      new LexicographicLongConverter();

  /**
   * Converts {@link Instant} to a {@code Long} representing its millis-since-epoch,
   * but shifted so that the byte representation of negative values are lexicographically
   * ordered before the byte representation of positive values.
   *
   * <p>This deliberately utilizes the well-defined overflow for {@code Long} values.
   * See http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.18.2
   */
  private static class LexicographicLongConverter extends Converter<Instant, Long> {

    @Override
    protected Long doForward(Instant instant) {
      return instant.getMillis() - Long.MIN_VALUE;
    }

    @Override
    protected Instant doBackward(Long shiftedMillis) {
      return new Instant(shiftedMillis + Long.MIN_VALUE);
    }
  }

  @Override
  public void encode(Instant value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    LONG_CODER.encode(ORDER_PRESERVING_CONVERTER.convert(value), outStream, context);
  }

  @Override
  public Instant decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    return ORDER_PRESERVING_CONVERTER.reverse().convert(LONG_CODER.decode(inStream, context));
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
   * @return {@code true}. The byte size for a big endian long is a constant.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(Instant value, Context context) {
    return LONG_CODER.isRegisterByteSizeObserverCheap(
        ORDER_PRESERVING_CONVERTER.convert(value), context);
  }

  @Override
  public void registerByteSizeObserver(
      Instant value, ElementByteSizeObserver observer, Context context) throws Exception {
    LONG_CODER.registerByteSizeObserver(
        ORDER_PRESERVING_CONVERTER.convert(value), observer, context);
  }

  @Override
  public TypeDescriptor<Instant> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
