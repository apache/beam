/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * A source that reads from a shuffled dataset and yields key-grouped
 * data.  Like {@link GroupingShuffleReader}, except that it injects
 * an error in the counter tracking how many bytes are read.  This
 * class is solely used to test the shuffle sanity check mechanism.
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
      CounterSet.AddCounterMutator addCounterMutator,
      String operationName,
      boolean sortValues)
      throws Exception {
    super(options, shuffleReaderConfig, startShufflePosition, stopShufflePosition, coder,
        executionContext, addCounterMutator, operationName, sortValues);
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
