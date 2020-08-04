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

import java.util.Random;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterBackedElementByteSizeObserver;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An {@link ElementCounter} that counts output objects, bytes, and mean bytes. */
public class OutputObjectAndByteCounter implements ElementCounter {
  // Might be null, e.g., undeclared outputs will not have an
  // elementByteSizeObservable.
  private final ElementByteSizeObservable<Object> elementByteSizeObservable;
  private final CounterFactory counterFactory;

  private Random randomGenerator = new Random();

  // Lowest sampling probability: 0.001%.
  private static final int SAMPLING_TOKEN_UPPER_BOUND = 1000000;
  private static final int SAMPLING_CUTOFF = 10;
  private int samplingToken = 0;

  private Counter<Long, ?> objectCount = null;
  private Counter<Long, Long> byteCount = null;
  private Counter<Long, CounterMean<Long>> meanByteCount = null;
  private CounterBackedElementByteSizeObserver byteCountObserver = null;
  private CounterBackedElementByteSizeObserver meanByteCountObserver = null;
  private int samplingTokenUpperBound = SAMPLING_TOKEN_UPPER_BOUND;

  // A variant which does not take a {@link NameContext} for flume.
  @SuppressWarnings("unchecked")
  public OutputObjectAndByteCounter(
      ElementByteSizeObservable<?> elementByteSizeObservable, CounterFactory counterFactory) {
    this(elementByteSizeObservable, counterFactory, null);
  }

  @SuppressWarnings("unchecked")
  public OutputObjectAndByteCounter(
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      @Nullable NameContext nameContext) {
    this.elementByteSizeObservable = (ElementByteSizeObservable<Object>) elementByteSizeObservable;
    this.counterFactory = counterFactory;
  }

  /** Count output objects. */
  public OutputObjectAndByteCounter countObject(String objectCounterName) {
    objectCount = counterFactory.longSum(getCounterName(objectCounterName));
    return this;
  }

  /** Count output bytes. */
  public OutputObjectAndByteCounter countBytes(String bytesCounterName) {
    if (elementByteSizeObservable != null) {
      byteCount = counterFactory.longSum(getCounterName(bytesCounterName));
      byteCountObserver = new CounterBackedElementByteSizeObserver(byteCount);
    }
    return this;
  }

  /** Count output mean byte. */
  public OutputObjectAndByteCounter countMeanByte(String meanByteCounterName) {
    if (elementByteSizeObservable != null) {
      meanByteCount = counterFactory.longMean(getCounterName(meanByteCounterName));
      meanByteCountObserver = new CounterBackedElementByteSizeObserver(meanByteCount);
    }
    return this;
  }

  /** Sets the minimum sampling rate to the inverse of the given value. */
  public OutputObjectAndByteCounter setSamplingPeriod(int period) {
    this.samplingTokenUpperBound = period * SAMPLING_CUTOFF;
    return this;
  }

  public Counter<Long, ?> getObjectCount() {
    return objectCount;
  }

  public Counter<Long, Long> getByteCount() {
    return byteCount;
  }

  public Counter<Long, CounterMean<Long>> getMeanByteCount() {
    return meanByteCount;
  }

  @Override
  public void update(Object elem) throws Exception {
    // Increment object counter.
    if (objectCount != null) {
      objectCount.addValue(1L);
    }

    // Increment byte counter.
    if ((byteCountObserver != null || meanByteCountObserver != null)
        && (sampleElement() || elementByteSizeObservable.isRegisterByteSizeObserverCheap(elem))) {
      if (byteCountObserver != null) {
        byteCountObserver.setScalingFactor(
            Math.max(samplingToken, SAMPLING_CUTOFF) / (double) SAMPLING_CUTOFF);
        elementByteSizeObservable.registerByteSizeObserver(elem, byteCountObserver);
      }
      if (meanByteCountObserver != null) {
        elementByteSizeObservable.registerByteSizeObserver(elem, meanByteCountObserver);
      }

      if (byteCountObserver != null && !byteCountObserver.getIsLazy()) {
        byteCountObserver.advance();
      }
      if (meanByteCountObserver != null && !meanByteCountObserver.getIsLazy()) {
        meanByteCountObserver.advance();
      }
    }
  }

  @Override
  public void finishLazyUpdate(Object elem) {
    // Advance lazy ElementByteSizeObservers, if any.
    // Note that user's code is allowed to store the element of one
    // DoFn.processElement() call and access it later on. We are still
    // calling next() here, causing an update to byteCount. If user's
    // code really accesses more element's pieces later on, their byte
    // count would accrue against a future element. This is not ideal,
    // but still approximately correct.
    if (byteCountObserver != null && byteCountObserver.getIsLazy()) {
      byteCountObserver.advance();
    }
    if (meanByteCountObserver != null && meanByteCountObserver.getIsLazy()) {
      meanByteCountObserver.advance();
    }
  }

  protected boolean sampleElement() {
    // Sampling probability decreases as the element count is increasing.
    // We unconditionally sample the first samplingCutoff elements. For the
    // next samplingCutoff elements, the sampling probability drops from 100%
    // to 50%. The probability of sampling the Nth element is:
    // min(1, samplingCutoff / N), with an additional lower bound of
    // samplingCutoff / samplingTokenUpperBound. This algorithm may be refined
    // later.
    samplingToken = Math.min(samplingToken + 1, samplingTokenUpperBound);
    return randomGenerator.nextInt(samplingToken) < SAMPLING_CUTOFF;
  }

  public OutputObjectAndByteCounter setRandom(Random random) {
    this.randomGenerator = random;
    return this;
  }

  private CounterName getCounterName(String name) {
    // TODO: use original name from the NameContext
    return CounterName.named(name);
  }
}
