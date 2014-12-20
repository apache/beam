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
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.BatchingShuffleEntryReader;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntryReader;
import com.google.cloud.dataflow.sdk.values.KV;

import java.io.IOException;
import java.util.Iterator;

/**
 * A source that reads from a key-sharded dataset, and returns KVs without
 * any values grouping.
 *
 * @param <K> the type of the keys read from the shuffle
 * @param <V> the type of the values read from the shuffle
 */
public class PartitioningShuffleReader<K, V> extends Reader<WindowedValue<KV<K, V>>> {
  final byte[] shuffleReaderConfig;
  final String startShufflePosition;
  final String stopShufflePosition;
  Coder<K> keyCoder;
  WindowedValueCoder<V> windowedValueCoder;

  public PartitioningShuffleReader(PipelineOptions options, byte[] shuffleReaderConfig,
      String startShufflePosition, String stopShufflePosition, Coder<WindowedValue<KV<K, V>>> coder)
      throws Exception {
    this.shuffleReaderConfig = shuffleReaderConfig;
    this.startShufflePosition = startShufflePosition;
    this.stopShufflePosition = stopShufflePosition;
    initCoder(coder);
  }

  /**
   * Given a {@code WindowedValueCoder<KV<K, V>>}, splits it into a coder for K
   * and a {@code WindowedValueCoder<V>} with the same kind of windows.
   */
  private void initCoder(Coder<WindowedValue<KV<K, V>>> coder) throws Exception {
    if (!(coder instanceof WindowedValueCoder)) {
      throw new Exception("unexpected kind of coder for WindowedValue: " + coder);
    }
    WindowedValueCoder<KV<K, V>> windowedElemCoder = ((WindowedValueCoder<KV<K, V>>) coder);
    Coder<KV<K, V>> elemCoder = windowedElemCoder.getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception("unexpected kind of coder for elements read from "
          + "a key-partitioning shuffle: " + elemCoder);
    }
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) elemCoder;
    this.keyCoder = kvCoder.getKeyCoder();
    windowedValueCoder = windowedElemCoder.withValueCoder(kvCoder.getValueCoder());
  }

  @Override
  public ReaderIterator<WindowedValue<KV<K, V>>> iterator() throws IOException {
    Preconditions.checkArgument(shuffleReaderConfig != null);
    return iterator(new BatchingShuffleEntryReader(
        new ChunkingShuffleBatchReader(new ApplianceShuffleReader(shuffleReaderConfig))));
  }

  ReaderIterator<WindowedValue<KV<K, V>>> iterator(ShuffleEntryReader reader) throws IOException {
    return new PartitioningShuffleReaderIterator(reader);
  }

  /**
   * A ReaderIterator that reads from a ShuffleEntryReader,
   * extracts K and {@code WindowedValue<V>}, and returns a constructed
   * {@code WindowedValue<KV>}.
   */
  class PartitioningShuffleReaderIterator extends AbstractReaderIterator<WindowedValue<KV<K, V>>> {
    Iterator<ShuffleEntry> iterator;

    PartitioningShuffleReaderIterator(ShuffleEntryReader reader) {
      this.iterator = reader.read(
          ByteArrayShufflePosition.fromBase64(startShufflePosition),
          ByteArrayShufflePosition.fromBase64(stopShufflePosition));
    }

    @Override
    public boolean hasNext() throws IOException {
      return iterator.hasNext();
    }

    @Override
    public WindowedValue<KV<K, V>> next() throws IOException {
      ShuffleEntry record = iterator.next();
      K key = CoderUtils.decodeFromByteArray(keyCoder, record.getKey());
      WindowedValue<V> windowedValue =
          CoderUtils.decodeFromByteArray(windowedValueCoder, record.getValue());
      notifyElementRead(record.length());
      return WindowedValue.of(KV.of(key, windowedValue.getValue()), windowedValue.getTimestamp(),
          windowedValue.getWindows());
    }
  }
}
