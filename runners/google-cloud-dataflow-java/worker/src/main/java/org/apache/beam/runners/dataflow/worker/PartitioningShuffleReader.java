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
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A source that reads from a key-sharded dataset, and returns KVs without any values grouping.
 *
 * @param <K> the type of the keys read from the shuffle
 * @param <V> the type of the values read from the shuffle
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PartitioningShuffleReader<K, V> extends NativeReader<WindowedValue<KV<K, V>>> {
  final byte[] shuffleReaderConfig;
  final String startShufflePosition;
  final String stopShufflePosition;
  private final BatchModeExecutionContext executionContext;
  private final DataflowOperationContext operationContext;
  Coder<K> keyCoder;
  WindowedValueCoder<V> windowedValueCoder;

  public PartitioningShuffleReader(
      PipelineOptions options,
      byte[] shuffleReaderConfig,
      String startShufflePosition,
      String stopShufflePosition,
      Coder<WindowedValue<KV<K, V>>> coder,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    this.shuffleReaderConfig = shuffleReaderConfig;
    this.startShufflePosition = startShufflePosition;
    this.stopShufflePosition = stopShufflePosition;
    this.executionContext = executionContext;
    this.operationContext = operationContext;
    initCoder(coder);
  }

  /**
   * Given a {@code WindowedValueCoder<KV<K, V>>}, splits it into a coder for K and a {@code
   * WindowedValueCoder<V>} with the same kind of windows.
   */
  private void initCoder(Coder<WindowedValue<KV<K, V>>> coder) throws Exception {
    if (!(coder instanceof WindowedValueCoder)) {
      throw new Exception("unexpected kind of coder for WindowedValue: " + coder);
    }
    WindowedValueCoder<KV<K, V>> windowedElemCoder = ((WindowedValueCoder<KV<K, V>>) coder);
    Coder<KV<K, V>> elemCoder = windowedElemCoder.getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception(
          "unexpected kind of coder for elements read from "
              + "a key-partitioning shuffle: "
              + elemCoder);
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) elemCoder;
    this.keyCoder = kvCoder.getKeyCoder();
    windowedValueCoder = windowedElemCoder.withValueCoder(kvCoder.getValueCoder());
  }

  @Override
  public NativeReaderIterator<WindowedValue<KV<K, V>>> iterator() throws IOException {
    return iterator(
        new ApplianceShuffleEntryReader(
            shuffleReaderConfig, executionContext, operationContext, false /* no caching */));
  }

  /**
   * Creates an iterator on top of the given entry reader.
   *
   * <p>Takes "ownership" of the reader: closes the reader once the iterator is closed.
   */
  PartitioningShuffleReaderIterator<K, V> iterator(ShuffleEntryReader reader) {
    return new PartitioningShuffleReaderIterator<>(this, reader);
  }

  /**
   * A ReaderIterator that reads from a ShuffleEntryReader, extracts K and {@code WindowedValue<V>},
   * and returns a constructed {@code WindowedValue<KV>}.
   */
  @VisibleForTesting
  static class PartitioningShuffleReaderIterator<K, V>
      extends NativeReaderIterator<WindowedValue<KV<K, V>>> {
    private Iterator<ShuffleEntry> iterator;
    private WindowedValue<KV<K, V>> current;
    private PartitioningShuffleReader<K, V> shuffleReader;
    private ShuffleEntryReader entryReader;

    PartitioningShuffleReaderIterator(
        PartitioningShuffleReader<K, V> shuffleReader, ShuffleEntryReader entryReader) {
      this.iterator =
          entryReader.read(
              shuffleReader.startShufflePosition == null
                  ? null
                  : ByteArrayShufflePosition.fromBase64(shuffleReader.startShufflePosition),
              shuffleReader.stopShufflePosition == null
                  ? null
                  : ByteArrayShufflePosition.fromBase64(shuffleReader.stopShufflePosition));
      this.shuffleReader = shuffleReader;
      this.entryReader = entryReader;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (!iterator.hasNext()) {
        current = null;
        return false;
      }
      ShuffleEntry record = iterator.next();
      K key = CoderUtils.decodeFromByteString(shuffleReader.keyCoder, record.getKey());
      WindowedValue<V> windowedValue =
          CoderUtils.decodeFromByteString(shuffleReader.windowedValueCoder, record.getValue());
      shuffleReader.notifyElementRead(record.length());
      current = windowedValue.withValue(KV.of(key, windowedValue.getValue()));
      return true;
    }

    @Override
    public WindowedValue<KV<K, V>> getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      entryReader.close();
    }
  }
}
