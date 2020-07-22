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

import java.io.IOException;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounterFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A source that reads from a shuffled dataset and yields key-grouped data. Like {@link
 * GroupingShuffleReader}, except that it injects an error in the counter tracking how many bytes
 * are read. This class is solely used to test the shuffle sanity check mechanism.
 *
 * @param <K> the type of the keys read from the shuffle
 * @param <V> the type of the values read from the shuffle
 */
@VisibleForTesting
class GroupingShuffleReaderWithFaultyBytesReadCounter<K, V> extends GroupingShuffleReader<K, V> {
  public GroupingShuffleReaderWithFaultyBytesReadCounter(
      PipelineOptions options,
      byte[] shuffleReaderConfig,
      @Nullable String startShufflePosition,
      @Nullable String stopShufflePosition,
      Coder<WindowedValue<KV<K, Iterable<V>>>> coder,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext,
      boolean sortValues)
      throws Exception {
    super(
        options,
        shuffleReaderConfig,
        startShufflePosition,
        stopShufflePosition,
        coder,
        executionContext,
        operationContext,
        ShuffleReadCounterFactory.INSTANCE,
        sortValues);
  }

  @Override
  public GroupingShuffleReaderIterator<K, V> iterator() throws IOException {
    // This causes perOperationPerDatasetBytesCounter to be initialized.
    GroupingShuffleReaderIterator<K, V> it = super.iterator();

    // Inject an error in the counter tracking how many bytes are read
    // from this reader's data source.
    perOperationPerDatasetBytesCounter.addValue(1L);
    return it;
  }
}
