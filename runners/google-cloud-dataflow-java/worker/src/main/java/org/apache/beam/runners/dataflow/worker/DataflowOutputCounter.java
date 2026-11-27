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
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

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

  // Concrete WindowedValues classes that always contain exactly one window. Checking exact Class
  // identity avoids HotSpot's _secondary_super_cache ping-pong (JDK-8180450) caused by back-to-back
  // interface checks (`instanceof SingleWindowedValue` followed by `checkcast WindowedValue` in
  // SimpleParDoFn.processElement) across concurrent worker threads, while avoiding
  // TimestampedValueInSingleWindow.getWindows()'s per-element Collections.singletonList allocation.
  //
  // TODO: Once Beam is on JDK 24, we can revert to the simpler instanceof call.
  private static final Class<?> VALUE_IN_GLOBAL_WINDOW_CLASS =
      WindowedValues.valueInGlobalWindow(null).getClass();
  private static final Class<?> TIMESTAMPED_VALUE_IN_GLOBAL_WINDOW_CLASS =
      WindowedValues.timestampedValueInGlobalWindow(null, Instant.EPOCH).getClass();
  private static final Class<?> TIMESTAMPED_VALUE_IN_SINGLE_WINDOW_CLASS =
      WindowedValues.of(
              null,
              Instant.EPOCH,
              new IntervalWindow(Instant.EPOCH, Instant.EPOCH),
              PaneInfo.NO_FIRING)
          .getClass();

  private final OutputObjectAndByteCounter objectAndByteCounter;
  private final Counter<Long, ?> elementCount;
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
    // TODO: use the name context to name the counter
    this.elementCount =
        counterFactory.longSum(CounterName.named(outputName + ELEMENT_COUNTER_NAME));
  }

  static boolean isKnownGlobalWindowedValue(Object elem) {
    Class<?> cls = elem.getClass();
    return cls == TIMESTAMPED_VALUE_IN_GLOBAL_WINDOW_CLASS || cls == VALUE_IN_GLOBAL_WINDOW_CLASS;
  }

  private static boolean isKnownSingleWindowedValue(Object elem) {
    Class<?> cls = elem.getClass();
    return cls == TIMESTAMPED_VALUE_IN_SINGLE_WINDOW_CLASS
        || cls == TIMESTAMPED_VALUE_IN_GLOBAL_WINDOW_CLASS
        || cls == VALUE_IN_GLOBAL_WINDOW_CLASS;
  }

  @Override
  public void update(Object elem) throws Exception {
    objectAndByteCounter.update(elem);
    if (isKnownSingleWindowedValue(elem)) {
      elementCount.addValue(1L);
    } else {
      updateNonSingleWindow(elem);
    }
  }

  private void updateNonSingleWindow(Object elem) {
    WindowedValue<?> windowedValue = (WindowedValue<?>) elem;
    long windowsSize = windowedValue.getWindows().size();
    if (windowsSize == 0) {
      updateEmptyWindows(windowedValue);
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
          if (isKnownSingleWindowedValue(element)) {
            totalElementCount += 1L;
          } else {
            long elementWindowsSize = element.getWindows().size();
            // Fan out for windows.
            totalElementCount += (elementWindowsSize == 0 ? 1L : elementWindowsSize);
          }
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
}
