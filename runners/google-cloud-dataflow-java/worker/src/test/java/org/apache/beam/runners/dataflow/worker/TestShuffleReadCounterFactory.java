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

import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounterFactory;

/**
 * Factor class to create ShuffleReadCounters in test This class maintains a map of every
 * shuffleReadCounters created for each shuffle step. Note: There is one ShuffleReadCounter for each
 * GroupingShuffleReader associated with a unique GBK/shuffle.
 */
public class TestShuffleReadCounterFactory extends ShuffleReadCounterFactory {

  private TreeMap<String, ShuffleReadCounter> originalShuffleStepToCounter;

  public TestShuffleReadCounterFactory() {
    this.originalShuffleStepToCounter = new TreeMap<>();
  }

  @Override
  public ShuffleReadCounter create(
      String originalShuffleStepName,
      boolean experimentEnabled,
      Counter<Long, Long> legacyPerOperationPerDatasetBytesCounter) {
    ShuffleReadCounter src =
        new ShuffleReadCounter(
            originalShuffleStepName, experimentEnabled, legacyPerOperationPerDatasetBytesCounter);
    originalShuffleStepToCounter.put(originalShuffleStepName, src);
    return src;
  }

  public ShuffleReadCounter getOnlyShuffleReadCounterOrNull() {
    if (originalShuffleStepToCounter.size() == 1) {
      return originalShuffleStepToCounter.firstEntry().getValue();
    }
    return null;
  }

  public Map<String, ShuffleReadCounter> shuffleReadCounters() {
    return originalShuffleStepToCounter;
  }
}
