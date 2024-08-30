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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A source that reads from a shuffled dataset, without any key grouping. Returns just the values.
 * (This reader is for an UNGROUPED shuffle session.)
 *
 * @param <T> the type of the elements read from the source
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class UngroupedShuffleReader<T> extends NativeReader<T> {
  final byte[] shuffleReaderConfig;
  final String startShufflePosition;
  final String stopShufflePosition;
  final Coder<T> coder;
  private final BatchModeExecutionContext executionContext;
  private final DataflowOperationContext operationContext;

  public UngroupedShuffleReader(
      @SuppressWarnings("unused") PipelineOptions options,
      byte[] shuffleReaderConfig,
      @Nullable String startShufflePosition,
      @Nullable String stopShufflePosition,
      Coder<T> coder,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext) {
    this.shuffleReaderConfig = shuffleReaderConfig;
    this.startShufflePosition = startShufflePosition;
    this.stopShufflePosition = stopShufflePosition;
    this.coder = coder;
    this.executionContext = executionContext;
    this.operationContext = operationContext;
  }

  @Override
  public NativeReaderIterator<T> iterator() throws IOException {
    return iterator(
        new ApplianceShuffleEntryReader(
            shuffleReaderConfig, executionContext, operationContext, false /* no caching */));
  }

  /**
   * Creates an iterator on top of the given entry reader.
   *
   * <p>Takes "ownership" of the reader: closes the reader once the iterator is closed.
   */
  UngroupedShuffleReaderIterator<T> iterator(ShuffleEntryReader reader) {
    return new UngroupedShuffleReaderIterator<>(this, reader);
  }

  /** A ReaderIterator that reads from a ShuffleEntryReader and extracts just the values. */
  @VisibleForTesting
  static class UngroupedShuffleReaderIterator<T> extends NativeReaderIterator<T> {
    private Iterator<ShuffleEntry> iterator;
    private T current;
    private UngroupedShuffleReader<T> shuffleReader;
    private final ShuffleEntryReader entryReader;

    UngroupedShuffleReaderIterator(
        UngroupedShuffleReader<T> shuffleReader, ShuffleEntryReader entryReader) {
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
      // Throw away the primary and the secondary keys.
      ByteString value = record.getValue();
      shuffleReader.notifyElementRead(record.length());
      current = CoderUtils.decodeFromByteString(shuffleReader.coder, value);
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
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
