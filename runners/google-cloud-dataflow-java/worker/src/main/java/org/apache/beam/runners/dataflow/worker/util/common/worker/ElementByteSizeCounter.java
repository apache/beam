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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongConsumer;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * An {@link ElementCounter} that estimates encoded element byte sizes (with adaptive sampling) and
 * reports them to a {@link LongConsumer}, without registering a counter in a {@link
 * org.apache.beam.runners.dataflow.worker.counters.CounterSet}.
 */
public class ElementByteSizeCounter implements ElementCounter {

  // Lowest sampling probability: 0.001%.
  private static final int DEFAULT_SAMPLING_TOKEN_UPPER_BOUND = 1_000_000;
  private static final int SAMPLING_CUTOFF = 10;

  private final ElementByteSizeObservable<Object> elementByteSizeObservable;
  private final LongConsumer byteCountConsumer;
  private final int samplingTokenUpperBound;
  private ElementByteSizeObserver byteSizeObserver;
  private int samplingToken = 0;

  public ElementByteSizeCounter(
      ElementByteSizeObservable<?> elementByteSizeObservable, LongConsumer byteCountConsumer) {
    this(
        elementByteSizeObservable,
        byteCountConsumer,
        DEFAULT_SAMPLING_TOKEN_UPPER_BOUND / SAMPLING_CUTOFF);
  }

  @SuppressWarnings("unchecked")
  public ElementByteSizeCounter(
      ElementByteSizeObservable<?> elementByteSizeObservable,
      LongConsumer byteCountConsumer,
      int samplingPeriod) {
    checkArgument(samplingPeriod > 0, "samplingPeriod must be positive, got %s", samplingPeriod);
    this.elementByteSizeObservable =
        (ElementByteSizeObservable<Object>) checkNotNull(elementByteSizeObservable);
    this.byteCountConsumer = checkNotNull(byteCountConsumer);
    this.samplingTokenUpperBound = Math.multiplyExact(samplingPeriod, SAMPLING_CUTOFF);
    this.byteSizeObserver = createObserver(this.byteCountConsumer);
  }

  private static ElementByteSizeObserver createObserver(LongConsumer byteCountConsumer) {
    return new ElementByteSizeObserver() {
      @Override
      protected void reportElementSize(long elementByteSize) {
        byteCountConsumer.accept(elementByteSize);
      }
    };
  }

  @Override
  public void update(Object elem) throws Exception {
    if (sampleElement() || elementByteSizeObservable.isRegisterByteSizeObserverCheap(elem)) {
      byteSizeObserver.setScalingFactor(
          Math.max(samplingToken, SAMPLING_CUTOFF) / (double) SAMPLING_CUTOFF);
      try {
        elementByteSizeObservable.registerByteSizeObserver(elem, byteSizeObserver);
      } catch (Exception | Error e) {
        // Discard any partial byte count or lazy flag recorded before the failure.
        byteSizeObserver = createObserver(byteCountConsumer);
        throw e;
      }
      if (!byteSizeObserver.getIsLazy()) {
        byteSizeObserver.advance();
      }
    }
  }

  @Override
  public void finishLazyUpdate(Object elem) {
    if (byteSizeObserver.getIsLazy()) {
      byteSizeObserver.advance();
    }
  }

  private boolean sampleElement() {
    samplingToken = Math.min(samplingToken + 1, samplingTokenUpperBound);
    return getRandom().nextInt(samplingToken) < SAMPLING_CUTOFF;
  }

  @VisibleForTesting
  protected Random getRandom() {
    return ThreadLocalRandom.current();
  }
}
