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
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.values.WindowedValue;

/**
 * A Dataflow output counter specific to Streaming pipelines. Unpacks {@link KeyedWorkItem}s in
 * empty windows (e.g. emitted by WindowingWindmillReader) and counts element windows (ignoring
 * timers) using lightweight metadata-only iteration via {@link
 * KeyedWorkItem#elementWindowsIterable()}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamingDataflowOutputCounter extends DataflowOutputCounter {

  public StreamingDataflowOutputCounter(
      String outputName, CounterFactory counterFactory, NameContext nameContext) {
    super(outputName, counterFactory, nameContext);
  }

  public StreamingDataflowOutputCounter(
      String outputName,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      NameContext nameContext) {
    super(outputName, elementByteSizeObservable, counterFactory, nameContext);
  }

  @Override
  protected void updateEmptyWindows(Object elem) {
    Object value = ((WindowedValue<?>) elem).getValue();
    if (value instanceof KeyedWorkItem<?, ?>) {
      KeyedWorkItem<?, ?> keyedWorkItem = (KeyedWorkItem<?, ?>) value;
      long totalElementCount = 0;
      // Iterate only through elementWindowsIterable and ignore timers in KeyedWorkItem.
      for (WindowedValue<?> element : keyedWorkItem.elementWindowsIterable()) {
        long elementWindowsSize = element.getWindows().size();
        // Fan out for windows.
        totalElementCount += (elementWindowsSize == 0 ? 1L : elementWindowsSize);
      }
      elementCount.addValue(totalElementCount);
    } else {
      elementCount.addValue(1L);
    }
  }
}
