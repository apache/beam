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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;
import com.google.cloud.dataflow.sdk.util.common.worker.ElementCounter;
import com.google.cloud.dataflow.sdk.util.common.worker.OutputObjectAndByteCounter;
import com.google.common.annotations.VisibleForTesting;

/**
 * A Dataflow-specific version of {@link ElementCounter}, which specifies
 * the object counter name differently as PhysicalElementCount.
 * Additionally, it counts element windows as ElementCount.
 */
public class DataflowOutputCounter implements ElementCounter {
  /** Number of physical element and multiple-window assignments that were serialized/processed. */
  private static final String OBJECT_COUNTER_NAME = "-PhysicalElementCount";
  /** Number of logical element and single window pairs that were processed. */
  private static final String ELEMENT_COUNTER_NAME = "-ElementCount";
  private static final String MEAN_BYTE_COUNTER_NAME = "-MeanByteCount";

  private OutputObjectAndByteCounter objectAndByteCounter;
  private Counter<Long> elementCount;

  public DataflowOutputCounter(String outputName, CounterSet.AddCounterMutator addCounterMutator) {
    this(outputName, null, addCounterMutator);
  }

  public DataflowOutputCounter(
      String outputName, ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterSet.AddCounterMutator addCounterMutator) {
    objectAndByteCounter =
        new OutputObjectAndByteCounter(elementByteSizeObservable, addCounterMutator);
    objectAndByteCounter.countObject(outputName + OBJECT_COUNTER_NAME);
    objectAndByteCounter.countMeanByte(outputName + MEAN_BYTE_COUNTER_NAME);
    elementCount =
        addCounterMutator.addCounter(Counter.longs(outputName + ELEMENT_COUNTER_NAME, SUM));
  }

  @Override
  public void update(Object elem) throws Exception {
    objectAndByteCounter.update(elem);
    long windowsSize = ((WindowedValue<?>) elem).getWindows().size();
    if (windowsSize == 0) {
      // GroupingShuffleReader produces ValueInEmptyWindows.
      // For now, we count the element at least once to keep the current counter
      // behavior.
      elementCount.addValue(1L);
    } else {
      elementCount.addValue(windowsSize);
    }
  }

  @Override
  public void finishLazyUpdate(Object elem) {
    objectAndByteCounter.finishLazyUpdate(elem);
  }

  @VisibleForTesting
  static String getElementCounterName(String prefix) {
    return prefix + ELEMENT_COUNTER_NAME;
  }

  @VisibleForTesting
  static String getObjectCounterName(String prefix) {
    return prefix + OBJECT_COUNTER_NAME;
  }

  @VisibleForTesting
  static String getMeanByteCounterName(String prefix) {
    return prefix + MEAN_BYTE_COUNTER_NAME;
  }
}
