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
package org.apache.beam.runners.kafka.streams.translation;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Executes a {@code GroupByKey} (GlobalWindow, default trigger, no allowed lateness).
 *
 * <p>Records arrive on the repartition topic keyed by the encoded Beam key, so every value of a key
 * is co-located here. Each value is appended to a per-key buffer in a Kafka Streams state store.
 * Watermark reports are fed to a {@link WatermarkManager}; when the input watermark reaches {@link
 * BoundedWindow#TIMESTAMP_MAX_VALUE} (the end of the global window) every buffered key is emitted
 * once as {@code KV<K, Iterable<V>>} and the buffer cleared, then the watermark is forwarded
 * downstream.
 *
 * <p>Buffering whole value lists and re-encoding on each append is O(n^2) per key; fine for this
 * first GroupByKey, and replaced when this moves to runner-core {@code GroupAlsoByWindow}.
 */
class GroupByKeyProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private final String stateStoreName;
  private final Coder<Object> keyCoder;
  private final IterableCoder<@Nullable Object> bufferCoder;

  private final WatermarkManager watermarkManager = new WatermarkManager();
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  // The global window fires exactly once, when the watermark first reaches its end. Later watermark
  // reports (e.g. the same terminal watermark broadcast across repartition partitions) must not
  // re-fire. This flag is in-memory only; restart correctness comes from the state store plus
  // exactly-once-v2: the buffered values and consumer offsets are committed atomically, and the
  // store is empty once a key has fired, so a restart cannot double-emit. Persisting watermark
  // holds is part of the separate WatermarkManager persistence work, not this initial GroupByKey.
  private boolean fired = false;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;
  private @Nullable KeyValueStore<byte[], byte[]> store;

  GroupByKeyProcessor(
      String stateStoreName, Coder<Object> keyCoder, Coder<@Nullable Object> valueCoder) {
    this.stateStoreName = stateStoreName;
    this.keyCoder = keyCoder;
    this.bufferCoder = IterableCoder.of(valueCoder);
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    this.store = context.getStateStore(stateStoreName);
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    KStreamsPayload<?> payload = record.value();
    if (payload.isData()) {
      byte[] encodedKey = record.key();
      Object element = payload.getData().getValue();
      if (encodedKey == null || element == null) {
        throw new IllegalStateException("GroupByKey data record is missing its key or value");
      }
      appendValue(encodedKey, element);
      return;
    }
    WatermarkPayload report = payload.asWatermark();
    watermarkManager.observe(
        report.getSourcePartition(),
        new Instant(report.getWatermarkMillis()),
        report.getTotalSourcePartitions());
    Instant advanced = watermarkManager.advance();
    if (!fired && !advanced.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      fireAll(record);
      fired = true;
    }
    if (advanced.isAfter(lastForwardedWatermark)) {
      lastForwardedWatermark = advanced;
      forwardWatermark(record, advanced.getMillis());
    }
  }

  private void appendValue(byte[] encodedKey, Object kvObject) {
    KV<?, ?> kv = (KV<?, ?>) kvObject;
    KeyValueStore<byte[], byte[]> kvStore = checkInitialized(store);
    byte[] existing = kvStore.get(encodedKey);
    List<@Nullable Object> values = existing == null ? new ArrayList<>() : decodeBuffer(existing);
    values.add(kv.getValue());
    kvStore.put(encodedKey, encodeBuffer(values));
  }

  private void fireAll(Record<byte[], KStreamsPayload<?>> trigger) {
    // NOTE: this emits every buffered key in a single watermark turn. For a very large key space
    // that risks memory pressure and exceeding the poll / transaction timeout. Acceptable for this
    // initial GlobalWindow GroupByKey (fire once at end of input); incremental, timer-driven output
    // via runner-core GroupAlsoByWindow lands with the windowing/timers work.
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    KeyValueStore<byte[], byte[]> kvStore = checkInitialized(store);
    List<byte[]> firedKeys = new ArrayList<>();
    try (KeyValueIterator<byte[], byte[]> it = kvStore.all()) {
      while (it.hasNext()) {
        org.apache.kafka.streams.KeyValue<byte[], byte[]> entry = it.next();
        Object key = decodeKey(entry.key);
        List<@Nullable Object> values = decodeBuffer(entry.value);
        // The pane fires at the end of the global window, so the grouped element carries the
        // window's max timestamp (END_OF_GLOBAL_WINDOW). Emitting at TIMESTAMP_MIN_VALUE (the
        // default of valueInGlobalWindow) would make the output appear arbitrarily late and be
        // dropped downstream once the watermark has advanced.
        WindowedValue<KV<Object, Iterable<@Nullable Object>>> output =
            WindowedValues.timestampedValueInGlobalWindow(
                KV.of(key, (Iterable<@Nullable Object>) values),
                GlobalWindow.INSTANCE.maxTimestamp());
        ctx.forward(
            new Record<byte[], KStreamsPayload<?>>(
                entry.key, KStreamsPayload.data(output), trigger.timestamp()));
        firedKeys.add(entry.key);
      }
    }
    for (byte[] key : firedKeys) {
      kvStore.delete(key);
    }
  }

  private void forwardWatermark(Record<byte[], KStreamsPayload<?>> trigger, long watermarkMillis) {
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    // GroupByKey is a single logical source for the next stage; report it as partition 0 of 1.
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            trigger.key(), KStreamsPayload.watermark(watermarkMillis, 0, 1), trigger.timestamp()));
  }

  private byte[] encodeBuffer(List<@Nullable Object> values) {
    try {
      return CoderUtils.encodeToByteArray(bufferCoder, values);
    } catch (CoderException e) {
      throw new RuntimeException("Failed to encode GroupByKey value buffer", e);
    }
  }

  private List<@Nullable Object> decodeBuffer(byte[] bytes) {
    try {
      List<@Nullable Object> values = new ArrayList<>();
      for (@Nullable Object value : CoderUtils.decodeFromByteArray(bufferCoder, bytes)) {
        values.add(value);
      }
      return values;
    } catch (CoderException e) {
      throw new RuntimeException("Failed to decode GroupByKey value buffer", e);
    }
  }

  private Object decodeKey(byte[] bytes) {
    try {
      return CoderUtils.decodeFromByteArray(keyCoder, bytes);
    } catch (CoderException e) {
      throw new RuntimeException("Failed to decode GroupByKey key", e);
    }
  }

  private static <T> T checkInitialized(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("GroupByKeyProcessor used before init()");
    }
    return value;
  }
}
