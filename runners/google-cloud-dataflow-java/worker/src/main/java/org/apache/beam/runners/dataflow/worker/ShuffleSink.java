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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;

/**
 * A sink that writes to a shuffle dataset.
 *
 * @param <T> the type of the elements written to the sink
 */
public class ShuffleSink<T> extends Sink<WindowedValue<T>> {
  enum ShuffleKind {
    UNGROUPED,
    PARTITION_KEYS,
    GROUP_KEYS,
    GROUP_KEYS_AND_SORT_VALUES
  }

  static final long SHUFFLE_WRITER_BUFFER_SIZE = 128 << 20;
  private static final byte[] NULL_VALUE_WITH_SIZE_PREFIX = new byte[] {0, 0, 0, 0};

  final byte[] shuffleWriterConfig;

  final ShuffleKind shuffleKind;

  final PipelineOptions options;

  private final BatchModeExecutionContext executionContext;
  private final DataflowOperationContext operationContext;
  private ExecutionStateTracker tracker;
  private ExecutionState writeState;

  boolean shardByKey;
  boolean groupValues;
  boolean sortValues;

  WindowedValueCoder<T> windowedElemCoder;
  WindowedValueCoder windowedValueCoder;
  Coder<T> elemCoder;
  Coder keyCoder;
  Coder valueCoder;
  Coder sortKeyCoder;
  Coder sortValueCoder;

  public static ShuffleKind parseShuffleKind(String shuffleKind) throws Exception {
    try {
      return Enum.valueOf(ShuffleKind.class, shuffleKind.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new Exception("unexpected shuffle_kind", e);
    }
  }

  public ShuffleSink(
      PipelineOptions options,
      byte[] shuffleWriterConfig,
      ShuffleKind shuffleKind,
      Coder<WindowedValue<T>> coder,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    this.shuffleWriterConfig = shuffleWriterConfig;
    this.shuffleKind = shuffleKind;
    this.options = options;
    this.executionContext = executionContext;
    this.operationContext = operationContext;
    this.writeState = operationContext.newExecutionState("write-shuffle");
    this.tracker = executionContext.getExecutionStateTracker();
    initCoder(coder);
  }

  private void initCoder(Coder<WindowedValue<T>> coder) throws Exception {
    switch (shuffleKind) {
      case UNGROUPED:
        this.shardByKey = false;
        this.groupValues = false;
        this.sortValues = false;
        break;
      case PARTITION_KEYS:
        this.shardByKey = true;
        this.groupValues = false;
        this.sortValues = false;
        break;
      case GROUP_KEYS:
        this.shardByKey = true;
        this.groupValues = true;
        this.sortValues = false;
        break;
      case GROUP_KEYS_AND_SORT_VALUES:
        this.shardByKey = true;
        this.groupValues = true;
        this.sortValues = true;
        break;
      default:
        throw new AssertionError("unexpected shuffle kind");
    }

    this.windowedElemCoder = (WindowedValueCoder<T>) coder;
    this.elemCoder = windowedElemCoder.getValueCoder();
    if (shardByKey) {
      if (!(elemCoder instanceof KvCoder)) {
        throw new Exception(
            String.format(
                "Unexpected kind of coder for elements written to a key-grouping shuffle %s.",
                elemCoder));
      }
      KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) elemCoder;
      this.keyCoder = kvCoder.getKeyCoder();
      this.valueCoder = kvCoder.getValueCoder();
      if (sortValues) {
        // TODO: Decide the representation of sort-keyed values.
        // For now, we'll just use KVs.
        if (!(valueCoder instanceof KvCoder)) {
          throw new Exception(
              String.format(
                  "Unexpected kind of coder for values written to a value-sorting shuffle %s.",
                  valueCoder));
        }
        KvCoder<?, ?> kvValueCoder = (KvCoder<?, ?>) valueCoder;
        this.sortKeyCoder = kvValueCoder.getKeyCoder();
        this.sortValueCoder = kvValueCoder.getValueCoder();
      } else {
        this.sortKeyCoder = null;
        this.sortValueCoder = null;
      }
      if (groupValues) {
        this.windowedValueCoder = null;
      } else {
        this.windowedValueCoder = this.windowedElemCoder.withValueCoder(this.valueCoder);
      }
    } else {
      this.keyCoder = null;
      this.valueCoder = null;
      this.sortKeyCoder = null;
      this.sortValueCoder = null;
      this.windowedValueCoder = null;
    }
  }

  /**
   * Returns a SinkWriter that allows writing to this ShuffleSink, using the given
   * ShuffleEntryWriter. The dataset ID is used to construct names of counterFactory that track
   * per-worker per-dataset bytes written to shuffle.
   */
  public SinkWriter<WindowedValue<T>> writer(ShuffleWriter writer, String datasetId) {
    return new ShuffleSinkWriter(writer, options, operationContext.counterFactory(), datasetId);
  }

  /** The SinkWriter for a ShuffleSink. */
  class ShuffleSinkWriter implements SinkWriter<WindowedValue<T>> {
    // This is the minimum size before we output a chunk except for
    // the final chunk when close() is called.
    private static final int MIN_OUTPUT_CHUNK_SIZE = 1 << 20;

    private ShuffleWriter writer;
    private long seqNum = 0;
    // How many bytes were written to a given shuffle session.
    private final Counter<Long, ?> perDatasetBytesCounter;
    private final RandomAccessData chunk;
    private boolean closed = false;

    ShuffleSinkWriter(
        ShuffleWriter writer,
        PipelineOptions options,
        CounterFactory counterFactory,
        String datasetId) {
      this.writer = writer;
      this.perDatasetBytesCounter =
          counterFactory.longSum(CounterName.named("dax-shuffle-" + datasetId + "-written-bytes"));
      // Initialize the chunk with the minimum size so we do not have to
      // "grow" the internal byte[] up to the minimum chunk size.
      this.chunk = new RandomAccessData(MIN_OUTPUT_CHUNK_SIZE);
    }

    @Override
    public long add(WindowedValue<T> windowedElem) throws IOException {
      T elem = windowedElem.getValue();
      long bytes = 0;
      if (shardByKey) {
        if (!(elem instanceof KV)) {
          throw new AssertionError(
              "expecting the values written to a key-grouping shuffle " + "to be KVs");
        }
        KV<?, ?> kv = (KV) elem;
        Object key = kv.getKey();
        Object value = kv.getValue();

        bytes += encodeToChunk(keyCoder, key);

        if (sortValues) {
          if (!(value instanceof KV)) {
            throw new AssertionError(
                "expecting the value parts of the KVs written to "
                    + "a value-sorting shuffle to also be KVs");
          }
          KV<?, ?> kvValue = (KV) value;
          Object sortKey = kvValue.getKey();
          Object sortValue = kvValue.getValue();

          // Sort values by key and then timestamp so that any GroupAlsoByWindows
          // can run more efficiently. We produce an order preserving encoding of this composite
          // key by concatenating the nested encoding of sortKey and outer encoding of the
          // timestamp. An alternative implementation would be to use OrderedCode but it
          // is unnecessary here because the nested encoding of sortKey achieves the same effect,
          // due to the nested encoding being a https://en.wikipedia.org/wiki/Prefix_code.
          // Move forward enough bytes so we can prefix the size on after performing the write
          int initialChunkSize = chunk.size();
          chunk.resetTo(initialChunkSize + Ints.BYTES);
          sortKeyCoder.encode(sortKey, chunk.asOutputStream());

          if (!windowedElem.getTimestamp().equals(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
            // Empty timestamp suffixes sort before all other sort value keys with
            // the same prefix. So We can omit this suffix for this common value here
            // for efficiency and only encode when its not the minimum timestamp.
            InstantCoder.of()
                .encode(windowedElem.getTimestamp(), chunk.asOutputStream(), Context.OUTER);
          }

          int elementSize = chunk.size() - initialChunkSize - Ints.BYTES;
          byte[] internalBytes = chunk.array();
          internalBytes[initialChunkSize] = (byte) ((elementSize >>> 24) & 0xFF);
          internalBytes[initialChunkSize + 1] = (byte) ((elementSize >>> 16) & 0xFF);
          internalBytes[initialChunkSize + 2] = (byte) ((elementSize >>> 8) & 0xFF);
          internalBytes[initialChunkSize + 3] = (byte) ((elementSize >>> 0) & 0xFF);
          bytes += elementSize;

          bytes += encodeToChunk(sortValueCoder, sortValue);
        } else if (groupValues) {
          // Sort values by timestamp so that GroupAlsoByWindows can run efficiently.
          if (windowedElem.getTimestamp().equals(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
            // Empty secondary keys sort before all other secondary keys, so we
            // can omit this common value here for efficiency.
            chunk.asOutputStream().write(NULL_VALUE_WITH_SIZE_PREFIX);
          } else {
            bytes += encodeToChunk(InstantCoder.of(), windowedElem.getTimestamp());
          }
          bytes += encodeToChunk(valueCoder, value);
        } else {
          chunk.asOutputStream().write(NULL_VALUE_WITH_SIZE_PREFIX);
          bytes += encodeToChunk(windowedValueCoder, windowedElem.withValue(value));
        }

      } else {
        // Not partitioning or grouping by key, just resharding values.
        // <key> is ignored, except by the shuffle splitter.  Use a seq#
        // as the key, so we can split records anywhere.  This also works
        // for writing a single-sharded ordered PCollection through a
        // shuffle, since the order of elements in the input will be
        // preserved in the output.
        bytes += encodeToChunk(BigEndianLongCoder.of(), seqNum++);
        chunk.asOutputStream().write(NULL_VALUE_WITH_SIZE_PREFIX);
        bytes += encodeToChunk(windowedElemCoder, windowedElem);
      }

      if (chunk.size() > MIN_OUTPUT_CHUNK_SIZE) {
        try (Closeable trackedWriteState = tracker.enterState(writeState)) {
          outputChunk();
        }
      }

      perDatasetBytesCounter.addValue(bytes);
      return bytes;
    }

    @Override
    public void close() throws IOException {
      try (Closeable trackedCloseState = tracker.enterState(writeState)) {
        outputChunk();
        writer.close();
      } finally {
        closed = true;
      }
    }

    @Override
    public void abort() throws IOException {
      if (!closed) {
        // ShuffleWriter extends AutoCloseable, so it may not be idempotent. Only close if it has
        // not already been closed.
        close();
      }
    }

    private void outputChunk() throws IOException {
      writer.write(Arrays.copyOf(chunk.array(), chunk.size()));
      chunk.resetTo(0);
    }

    private <EncodeT> int encodeToChunk(Coder<EncodeT> coder, EncodeT value) throws IOException {
      // Move forward enough bytes so we can prefix the size on after performing the write
      int initialChunkSize = chunk.size();
      chunk.resetTo(initialChunkSize + Ints.BYTES);
      coder.encode(value, chunk.asOutputStream(), Context.OUTER);
      int elementSize = chunk.size() - initialChunkSize - Ints.BYTES;

      byte[] internalBytes = chunk.array();
      internalBytes[initialChunkSize] = (byte) ((elementSize >>> 24) & 0xFF);
      internalBytes[initialChunkSize + 1] = (byte) ((elementSize >>> 16) & 0xFF);
      internalBytes[initialChunkSize + 2] = (byte) ((elementSize >>> 8) & 0xFF);
      internalBytes[initialChunkSize + 3] = (byte) ((elementSize >>> 0) & 0xFF);
      return elementSize;
    }
  }

  @Override
  public SinkWriter<WindowedValue<T>> writer() throws IOException {
    Preconditions.checkArgument(shuffleWriterConfig != null);
    ApplianceShuffleWriter applianceWriter =
        new ApplianceShuffleWriter(
            shuffleWriterConfig, SHUFFLE_WRITER_BUFFER_SIZE, operationContext);
    String datasetId = applianceWriter.getDatasetId();
    return writer(applianceWriter, datasetId);
  }
}
