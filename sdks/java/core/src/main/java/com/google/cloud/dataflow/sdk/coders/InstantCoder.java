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

import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.common.base.Converter;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Coder} for joda {@link Instant} that encodes it as a big endian {@link Long}
 * shifted such that lexicographic ordering of the bytes corresponds to chronological order.
 */
public class InstantCoder extends AtomicCoder<Instant> {

  @JsonCreator
  public static InstantCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final InstantCoder INSTANCE = new InstantCoder();

  private final BigEndianLongCoder longCoder = BigEndianLongCoder.of();

  private InstantCoder() {}

  /**
   * Converts {@link Instant} to a {@code Long} representing its millis-since-epoch,
   * but shifted so that the byte representation of negative values are lexicographically
   * ordered before the byte representation of positive values.
   *
   * <p>This deliberately utilizes the well-defined overflow for {@code Long} values.
   * See http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.18.2
   */
  private static final Converter<Instant, Long> ORDER_PRESERVING_CONVERTER =
      new Converter<Instant, Long>() {

        @Override
        protected Long doForward(Instant instant) {
          return instant.getMillis() - Long.MIN_VALUE;
        }

        @Override
        protected Instant doBackward(Long shiftedMillis) {
          return new Instant(shiftedMillis + Long.MIN_VALUE);
        }
  };

  @Override
  public void encode(Instant value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    longCoder.encode(ORDER_PRESERVING_CONVERTER.convert(value), outStream, context);
  }

  @Override
  public Instant decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    return ORDER_PRESERVING_CONVERTER.reverse().convert(longCoder.decode(inStream, context));
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
    return longCoder.isRegisterByteSizeObserverCheap(
        ORDER_PRESERVING_CONVERTER.convert(value), context);
  }

  @Override
  public void registerByteSizeObserver(
      Instant value, ElementByteSizeObserver observer, Context context) throws Exception {
    longCoder.registerByteSizeObserver(
        ORDER_PRESERVING_CONVERTER.convert(value), observer, context);
  }
}
