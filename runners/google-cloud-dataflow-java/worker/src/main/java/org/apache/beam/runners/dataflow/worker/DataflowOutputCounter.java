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
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A Dataflow-specific version of {@link ElementCounter}, which specifies the object counter name
 * differently as PhysicalElementCount. Additionally, it counts element windows as ElementCount.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DataflowOutputCounter implements ElementCounter {
  /** Number of physical element and multiple-window assignments that were serialized/processed. */
  private static final String OBJECT_COUNTER_NAME = "-PhysicalElementCount";
  /** Number of logical element and single window pairs that were processed. */
  private static final String ELEMENT_COUNTER_NAME = "-ElementCount";

  private static final String MEAN_BYTE_COUNTER_NAME = "-MeanByteCount";

  private OutputObjectAndByteCounter objectAndByteCounter;
  private Counter<Long, ?> elementCount;

  public DataflowOutputCounter(
      String outputName, CounterFactory counterFactory, NameContext nameContext) {
    this(outputName, null, counterFactory, nameContext);
  }

  public DataflowOutputCounter(
      String outputName,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      NameContext nameContext) {
    objectAndByteCounter =
        new OutputObjectAndByteCounter(elementByteSizeObservable, counterFactory, nameContext);
    objectAndByteCounter.countObject(outputName + OBJECT_COUNTER_NAME);
    objectAndByteCounter.countMeanByte(outputName + MEAN_BYTE_COUNTER_NAME);
    createElementCounter(counterFactory, outputName + ELEMENT_COUNTER_NAME);
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

  private void createElementCounter(CounterFactory factory, String name) {
    // TODO: use the name context to name the counter
    elementCount = factory.longSum(CounterName.named(name));
  }
}
