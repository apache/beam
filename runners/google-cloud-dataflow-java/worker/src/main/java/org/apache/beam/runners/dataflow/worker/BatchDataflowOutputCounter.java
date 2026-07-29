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
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;

/**
 * A Dataflow output counter specific to Batch pipelines. In batch pipelines, empty-window elements
 * (e.g. GroupingShuffleReader emitting KV<K, ValuesIterable>) represent a single PCollection
 * element output.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BatchDataflowOutputCounter extends DataflowOutputCounter {

  public BatchDataflowOutputCounter(
      String outputName, CounterFactory counterFactory, NameContext nameContext) {
    super(outputName, counterFactory, nameContext);
  }

  public BatchDataflowOutputCounter(
      String outputName,
      ElementByteSizeObservable<?> elementByteSizeObservable,
      CounterFactory counterFactory,
      NameContext nameContext) {
    super(outputName, elementByteSizeObservable, counterFactory, nameContext);
  }

  @Override
  protected void updateEmptyWindows(Object elem) {
    // Non-KeyedWorkItem wrapped in ValueInEmptyWindows (e.g. GroupingShuffleReader KV output for
    // Batch GBK)
    elementCount.addValue(1L);
  }
}
