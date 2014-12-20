/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.BatchingShuffleEntryReader;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntryReader;

import java.io.IOException;
import java.util.Iterator;

import javax.annotation.Nullable;

/**
 * A source that reads from a shuffled dataset, without any key grouping.
 * Returns just the values.  (This reader is for an UNGROUPED shuffle session.)
 *
 * @param <T> the type of the elements read from the source
 */
public class UngroupedShuffleReader<T> extends Reader<T> {
  final byte[] shuffleReaderConfig;
  final String startShufflePosition;
  final String stopShufflePosition;
  final Coder<T> coder;

  public UngroupedShuffleReader(PipelineOptions options, byte[] shuffleReaderConfig,
      @Nullable String startShufflePosition, @Nullable String stopShufflePosition, Coder<T> coder) {
    this.shuffleReaderConfig = shuffleReaderConfig;
    this.startShufflePosition = startShufflePosition;
    this.stopShufflePosition = stopShufflePosition;
    this.coder = coder;
  }

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    Preconditions.checkArgument(shuffleReaderConfig != null);
    return iterator(new BatchingShuffleEntryReader(
        new ChunkingShuffleBatchReader(new ApplianceShuffleReader(shuffleReaderConfig))));
  }

  ReaderIterator<T> iterator(ShuffleEntryReader reader) throws IOException {
    return new UngroupedShuffleReaderIterator(reader);
  }

  /**
   * A ReaderIterator that reads from a ShuffleEntryReader and extracts
   * just the values.
   */
  class UngroupedShuffleReaderIterator extends AbstractReaderIterator<T> {
    Iterator<ShuffleEntry> iterator;

    UngroupedShuffleReaderIterator(ShuffleEntryReader reader) throws IOException {
      this.iterator = reader.read(
          ByteArrayShufflePosition.fromBase64(startShufflePosition),
          ByteArrayShufflePosition.fromBase64(stopShufflePosition));
    }

    @Override
    public boolean hasNext() throws IOException {
      return iterator.hasNext();
    }

    @Override
    public T next() throws IOException {
      ShuffleEntry record = iterator.next();
      // Throw away the primary and the secondary keys.
      byte[] value = record.getValue();
      notifyElementRead(record.length());
      return CoderUtils.decodeFromByteArray(coder, value);
    }
  }
}
