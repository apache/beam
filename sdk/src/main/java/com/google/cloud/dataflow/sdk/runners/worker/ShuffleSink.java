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
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.KV;

import java.io.IOException;

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

  final byte[] shuffleWriterConfig;

  final ShuffleKind shuffleKind;

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

  public ShuffleSink(PipelineOptions options, byte[] shuffleWriterConfig, ShuffleKind shuffleKind,
      Coder<WindowedValue<T>> coder) throws Exception {
    this.shuffleWriterConfig = shuffleWriterConfig;
    this.shuffleKind = shuffleKind;
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
        throw new Exception("unexpected kind of coder for elements written to "
            + "a key-grouping shuffle");
      }
      KvCoder<?, ?> kvCoder = (KvCoder) elemCoder;
      this.keyCoder = kvCoder.getKeyCoder();
      this.valueCoder = kvCoder.getValueCoder();
      if (sortValues) {
        // TODO: Decide the representation of sort-keyed values.
        // For now, we'll just use KVs.
        if (!(valueCoder instanceof KvCoder)) {
          throw new Exception("unexpected kind of coder for values written to "
              + "a value-sorting shuffle");
        }
        KvCoder<?, ?> kvValueCoder = (KvCoder) valueCoder;
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
   * Returns a SinkWriter that allows writing to this ShuffleSink,
   * using the given ShuffleEntryWriter.
   */
  public SinkWriter<WindowedValue<T>> writer(ShuffleEntryWriter writer) throws IOException {
    return new ShuffleSinkWriter(writer);
  }

  /** The SinkWriter for a ShuffleSink. */
  class ShuffleSinkWriter implements SinkWriter<WindowedValue<T>> {
    ShuffleEntryWriter writer;
    long seqNum = 0;

    ShuffleSinkWriter(ShuffleEntryWriter writer) throws IOException {
      this.writer = writer;
    }

    @Override
    public long add(WindowedValue<T> windowedElem) throws IOException {
      byte[] keyBytes;
      byte[] secondaryKeyBytes;
      byte[] valueBytes;
      T elem = windowedElem.getValue();
      if (shardByKey) {
        if (!(elem instanceof KV)) {
          throw new AssertionError("expecting the values written to a key-grouping shuffle "
              + "to be KVs");
        }
        KV<?, ?> kv = (KV) elem;
        Object key = kv.getKey();
        Object value = kv.getValue();

        keyBytes = CoderUtils.encodeToByteArray(keyCoder, key);

        if (sortValues) {
          if (!(value instanceof KV)) {
            throw new AssertionError("expecting the value parts of the KVs written to "
                + "a value-sorting shuffle to also be KVs");
          }
          KV<?, ?> kvValue = (KV) value;
          Object sortKey = kvValue.getKey();
          Object sortValue = kvValue.getValue();

          // TODO: Need to coordinate with the
          // GroupingShuffleReader, to make sure it knows how to
          // reconstruct the value from the sortKeyBytes and
          // sortValueBytes.  Right now, it doesn't know between
          // sorting and non-sorting GBKs.
          secondaryKeyBytes = CoderUtils.encodeToByteArray(sortKeyCoder, sortKey);
          valueBytes = CoderUtils.encodeToByteArray(sortValueCoder, sortValue);

        } else if (groupValues) {
          // Sort values by timestamp so that GroupAlsoByWindows can run efficiently.
          if (windowedElem.getTimestamp().getMillis() == Long.MIN_VALUE) {
            // Empty secondary keys sort before all other secondary keys, so we
            // can omit this common value here for efficiency.
            secondaryKeyBytes = null;
          } else {
            secondaryKeyBytes =
                CoderUtils.encodeToByteArray(InstantCoder.of(), windowedElem.getTimestamp());
          }
          valueBytes = CoderUtils.encodeToByteArray(valueCoder, value);
        } else {
          secondaryKeyBytes = null;
          valueBytes = CoderUtils.encodeToByteArray(
              windowedValueCoder,
              WindowedValue.of(value, windowedElem.getTimestamp(), windowedElem.getWindows()));
        }

      } else {
        // Not partitioning or grouping by key, just resharding values.
        // <key> is ignored, except by the shuffle splitter.  Use a seq#
        // as the key, so we can split records anywhere.  This also works
        // for writing a single-sharded ordered PCollection through a
        // shuffle, since the order of elements in the input will be
        // preserved in the output.
        keyBytes = CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), seqNum++);

        secondaryKeyBytes = null;
        valueBytes = CoderUtils.encodeToByteArray(windowedElemCoder, windowedElem);
      }

      return writer.put(new ShuffleEntry(keyBytes, secondaryKeyBytes, valueBytes));
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  @Override
  public SinkWriter<WindowedValue<T>> writer() throws IOException {
    Preconditions.checkArgument(shuffleWriterConfig != null);
    return writer(new ChunkingShuffleEntryWriter(
        new ApplianceShuffleWriter(shuffleWriterConfig, SHUFFLE_WRITER_BUFFER_SIZE)));
  }
}
