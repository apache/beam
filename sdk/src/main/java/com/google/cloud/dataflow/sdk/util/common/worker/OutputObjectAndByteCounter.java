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

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import java.util.Random;

/**
 * An {@link ElementCounter} that counts output objects, bytes, and mean bytes.
 */
public class OutputObjectAndByteCounter implements ElementCounter {
  // Might be null, e.g., undeclared outputs will not have an
  // elementByteSizeObservable.
  private final ElementByteSizeObservable<Object> elementByteSizeObservable;
  private final CounterSet.AddCounterMutator addCounterMutator;

  private final Random randomGenerator = new Random();

  // Lowest sampling probability: 0.001%.
  private static final int SAMPLING_TOKEN_UPPER_BOUND = 1000000;
  private static final int SAMPLING_CUTOFF = 10;
  private int samplingToken = 0;

  private Counter<Long> objectCount = null;
  private Counter<Long> byteCount = null;
  private Counter<Long> meanByteCount = null;
  private ElementByteSizeObserver byteCountObserver = null;
  private ElementByteSizeObserver meanByteCountObserver = null;

  @SuppressWarnings("unchecked")
  public OutputObjectAndByteCounter(
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterSet.AddCounterMutator addCounterMutator) {
    this.elementByteSizeObservable = (ElementByteSizeObservable<Object>) elementByteSizeObservable;
    this.addCounterMutator = addCounterMutator;
  }

  /**
   * Count output objects.
   */
  public OutputObjectAndByteCounter countObject(String objectCounterName) {
    objectCount = addCounterMutator.addCounter(Counter.longs(objectCounterName, SUM));
    return this;
  }

  /**
   * Count output bytes.
   */
  public OutputObjectAndByteCounter countBytes(String bytesCounterName) {
    if (elementByteSizeObservable != null) {
      byteCount = addCounterMutator.addCounter(Counter.longs(bytesCounterName, SUM));
      byteCountObserver = new ElementByteSizeObserver(byteCount);
    }
    return this;
  }

  /**
   * Count output mean byte.
   */
  public OutputObjectAndByteCounter countMeanByte(String meanByteCounterName) {
    if (elementByteSizeObservable != null) {
      meanByteCount = addCounterMutator.addCounter(Counter.longs(meanByteCounterName, MEAN));
      meanByteCountObserver = new ElementByteSizeObserver(meanByteCount);
    }
    return this;
  }

  public Counter<Long> getObjectCount() {
    return objectCount;
  }

  public Counter<Long> getByteCount() {
    return byteCount;
  }

  public Counter<Long> getMeanByteCount() {
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
    samplingToken = Math.min(samplingToken + 1, SAMPLING_TOKEN_UPPER_BOUND);
    return randomGenerator.nextInt(samplingToken) < SAMPLING_CUTOFF;
  }
}
