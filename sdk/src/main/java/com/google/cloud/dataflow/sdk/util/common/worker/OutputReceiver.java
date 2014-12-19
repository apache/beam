/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Receiver that forwards each input it receives to each of a list of
 * output Receivers. Additionally, it tracks output counters, that is, size
 * information for elements passing through.
 */
public class OutputReceiver implements Receiver {
  private final String outputName;
  // Might be null, e.g., undeclared outputs will not have an
  // elementByteSizeObservable.
  private final ElementByteSizeObservable<Object> elementByteSizeObservable;
  private final Counter<Long> elementCount;
  private Counter<Long> byteCount = null;
  private Counter<Long> meanByteCount = null;
  private ElementByteSizeObserver byteCountObserver = null;
  private ElementByteSizeObserver meanByteCountObserver = null;
  private final List<Receiver> outputs = new ArrayList<>();
  private final Random randomGenerator = new Random();
  private int samplingToken = 0;
  private final int samplingTokenUpperBound = 1000000;  // Lowest sampling probability: 0.001%.
  private final int samplingCutoff = 10;

  public OutputReceiver(String outputName,
                        String counterPrefix,
                        CounterSet.AddCounterMutator addCounterMutator) {
    this(outputName, null, counterPrefix, addCounterMutator);
  }

  @SuppressWarnings("unchecked")
  public OutputReceiver(String outputName,
                        ElementByteSizeObservable<?> elementByteSizeObservable,
                        String counterPrefix,
                        CounterSet.AddCounterMutator addCounterMutator) {
    this.outputName = outputName;
    this.elementByteSizeObservable = (ElementByteSizeObservable<Object>) elementByteSizeObservable;

    elementCount = addCounterMutator.addCounter(
        Counter.longs(elementsCounterName(counterPrefix, outputName), SUM));

    if (elementByteSizeObservable != null) {
      String bytesCounterName = bytesCounterName(counterPrefix, outputName);
      if (bytesCounterName != null) {
        byteCount = addCounterMutator.addCounter(
            Counter.longs(bytesCounterName, SUM));
        byteCountObserver = new ElementByteSizeObserver(byteCount);
      }
      String meanBytesCounterName =
          meanBytesCounterName(counterPrefix, outputName);
      if (meanBytesCounterName != null) {
        meanByteCount = addCounterMutator.addCounter(
            Counter.longs(meanBytesCounterName, MEAN));
        meanByteCountObserver = new ElementByteSizeObserver(meanByteCount);
      }
    }
  }

  protected String elementsCounterName(String counterPrefix,
                                       String outputName) {
    return outputName + "-ElementCount";
  }
  protected String bytesCounterName(String counterPrefix,
                                    String outputName) {
    return null;
  }
  protected String meanBytesCounterName(String counterPrefix,
                                        String outputName) {
    return outputName + "-MeanByteCount";
  }

  /**
   * Adds a new receiver that this OutputReceiver forwards to.
   */
  public void addOutput(Receiver receiver) {
    outputs.add(receiver);
  }

  @Override
  public void process(Object elem) throws Exception {
    // Increment element counter.
    elementCount.addValue(1L);

    // Increment byte counter.
    boolean advanceByteCountObserver = false;
    boolean advanceMeanByteCountObserver = false;
    if ((byteCountObserver != null || meanByteCountObserver != null)
        && (sampleElement()
            || elementByteSizeObservable.isRegisterByteSizeObserverCheap(
                elem))) {

      if (byteCountObserver != null) {
        elementByteSizeObservable.registerByteSizeObserver(
            elem, byteCountObserver);
      }
      if (meanByteCountObserver != null) {
        elementByteSizeObservable.registerByteSizeObserver(
            elem, meanByteCountObserver);
      }

      if (byteCountObserver != null) {
        if (!byteCountObserver.getIsLazy()) {
          byteCountObserver.advance();
        } else {
          advanceByteCountObserver = true;
        }
      }
      if (meanByteCountObserver != null) {
        if (!meanByteCountObserver.getIsLazy()) {
          meanByteCountObserver.advance();
        } else {
          advanceMeanByteCountObserver = true;
        }
      }
    }

    // Fan-out.
    for (Receiver out : outputs) {
      if (out != null) {
        out.process(elem);
      }
    }

    // Advance lazy ElementByteSizeObservers, if any.
    // Note that user's code is allowed to store the element of one
    // DoFn.processElement() call and access it later on. We are still
    // calling next() here, causing an update to byteCount. If user's
    // code really accesses more element's pieces later on, their byte
    // count would accrue against a future element. This is not ideal,
    // but still approximately correct.
    if (advanceByteCountObserver) {
      byteCountObserver.advance();
    }
    if (advanceMeanByteCountObserver) {
      meanByteCountObserver.advance();
    }
  }

  public String getName() {
    return outputName;
  }

  public Counter<Long> getElementCount() {
    return elementCount;
  }

  public Counter<Long> getByteCount() {
    return byteCount;
  }

  public Counter<Long> getMeanByteCount() {
    return meanByteCount;
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
    return randomGenerator.nextInt(samplingToken) < samplingCutoff;
  }

  /** Invoked by tests only. */
  public int getReceiverCount() {
    return outputs.size();
  }

  /** Invoked by tests only. */
  public Receiver getOnlyReceiver() {
    if (outputs.size() != 1) {
      throw new AssertionError("only one receiver expected");
    }

    return outputs.get(0);
  }
}
