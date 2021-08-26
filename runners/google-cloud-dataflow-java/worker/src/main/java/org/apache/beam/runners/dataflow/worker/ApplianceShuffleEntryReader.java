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

import org.apache.beam.runners.dataflow.worker.util.common.worker.BatchingShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.CachingShuffleBatchReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleBatchReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An implementation of ShuffleEntryReader that uses ApplianceShuffleReader. */
public class ApplianceShuffleEntryReader implements ShuffleEntryReader {
  private ApplianceShuffleReader applianceShuffleReader;
  private ShuffleEntryReader entryReader;

  public ApplianceShuffleEntryReader(
      byte[] shuffleReaderConfig,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext,
      boolean cache) {
    Preconditions.checkArgument(shuffleReaderConfig != null);
    applianceShuffleReader = new ApplianceShuffleReader(shuffleReaderConfig, operationContext);

    ShuffleBatchReader batchReader =
        new ChunkingShuffleBatchReader(executionContext, operationContext, applianceShuffleReader);

    if (cache) {
      // Limit the size of the cache.
      final int maxBatches = 32;
      batchReader = new CachingShuffleBatchReader(batchReader, maxBatches);
    }
    entryReader = new BatchingShuffleEntryReader(batchReader);
  }

  @Override
  public Reiterator<ShuffleEntry> read(
      @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
    return entryReader.read(startPosition, endPosition);
  }

  @Override
  public void close() {
    applianceShuffleReader.close();
  }

  public String getDatasetId() {
    return applianceShuffleReader.getDatasetId();
  }
}
