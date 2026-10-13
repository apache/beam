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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A Dataflow-specific version of {@link ElementCounter}. It counts element windows as ElementCount.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
public class DataflowOutputCounter implements ElementCounter {
  /** Number of logical element and single window pairs that were processed. */
  private static final String ELEMENT_COUNTER_NAME = "-ElementCount";

  private static final String MEAN_BYTE_COUNTER_NAME = "-MeanByteCount";

  private OutputObjectAndByteCounter objectAndByteCounter;
  private Counter<Long, ?> elementCount;
  private final boolean isStreaming;

  public static DataflowOutputCounter create(
      String outputName,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      NameContext nameContext,
      boolean isStreaming) {
    return new DataflowOutputCounter(
        outputName, elementByteSizeObservable, counterFactory, nameContext, isStreaming);
  }

  public static DataflowOutputCounter create(
      String outputName,
      CounterFactory counterFactory,
      NameContext nameContext,
      boolean isStreaming) {
    return new DataflowOutputCounter(outputName, null, counterFactory, nameContext, isStreaming);
  }

  private DataflowOutputCounter(
      String outputName,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      NameContext nameContext,
      boolean isStreaming) {
    this.isStreaming = isStreaming;
    this.objectAndByteCounter =
        new OutputObjectAndByteCounter(elementByteSizeObservable, counterFactory, nameContext);
    this.objectAndByteCounter.countMeanByte(outputName + MEAN_BYTE_COUNTER_NAME);
    createElementCounter(counterFactory, outputName + ELEMENT_COUNTER_NAME);
  }

  @Override
  public void update(Object elem) throws Exception {
    objectAndByteCounter.update(elem);
    long windowsSize = ((WindowedValue<?>) elem).getWindows().size();
    if (windowsSize == 0) {
      updateEmptyWindows((WindowedValue<?>) elem);
    } else {
      // Standard WindowedValue.
      elementCount.addValue(windowsSize);
    }
  }

  private void updateEmptyWindows(WindowedValue<?> elem) {
    if (isStreaming) {
      Object value = elem.getValue();
      if (value instanceof KeyedWorkItem<?, ?>) {
        // KeyedWorkItem wrapped in ValueInEmptyWindows
        // (e.g. WindowingWindmillReader for Streaming GBK)
        KeyedWorkItem<?, ?> keyedWorkItem = (KeyedWorkItem<?, ?>) value;
        long totalElementCount = 0;
        // Iterate through elementWindowsIterable and ignore timers in KeyedWorkItem.
        // Uses lightweight metadata-only iteration without payload deserialization overhead.
        for (WindowedValue<?> element : keyedWorkItem.elementWindowsIterable()) {
          long elementWindowsSize = element.getWindows().size();
          // Fan out for windows.
          totalElementCount += (elementWindowsSize == 0 ? 1L : elementWindowsSize);
        }
        elementCount.addValue(totalElementCount);
      } else {
        // NOTE: in streaming mode, this should not normally happen.
        // Counting as 1 element serves as a fallback to maintain counter behavior without failing
        // execution.
        elementCount.addValue(1L);
      }
    } else {
      // Non-KeyedWorkItem wrapped in ValueInEmptyWindows
      // (e.g. GroupingShuffleReader KV output for Batch GBK)
      elementCount.addValue(1L);
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
  static String getMeanByteCounterName(String prefix) {
    return prefix + MEAN_BYTE_COUNTER_NAME;
  }

  private void createElementCounter(CounterFactory factory, String name) {
    // TODO: use the name context to name the counter
    elementCount = factory.longSum(CounterName.named(name));
  }
}
