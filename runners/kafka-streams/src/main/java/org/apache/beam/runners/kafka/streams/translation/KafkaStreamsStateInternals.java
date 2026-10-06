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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link StateInternals} for one key, backed by a Kafka Streams {@link KeyValueStore}.
 *
 * <p>Beam addresses a state cell by {@code (key, StateNamespace, StateTag)}; a windowed pipeline
 * puts each window's state in its own namespace. Every cell is stored as one entry in the shared
 * per-transform store under a composite byte key {@code len(key)|key | len(ns)|ns | len(tag)|tag},
 * so all cells for one Beam key share a prefix and a whole key's state can be range-scanned. The
 * value is the cell's contents encoded with its Beam {@link Coder}. Writing straight to the store
 * (rather than buffering and flushing) keeps this restart-safe for free: the store is changelogged
 * and, under exactly-once, its writes commit atomically with the input offsets.
 *
 * <p>Modeled on the Spark runner's {@code SparkStateInternals}; the difference is that each cell
 * reads and writes its own store entry instead of an in-memory table, so there is no separate
 * persist step.
 */
class KafkaStreamsStateInternals<K> implements StateInternals {

  /** The holds index is a set; only its keys carry information. */
  private static final byte[] EMPTY_VALUE = new byte[0];

  /**
   * Reads the minimum watermark hold held by any key and window, or {@code null} if none is held.
   * The index is ordered by hold time, so this is the first entry rather than a scan.
   */
  static @Nullable Instant minWatermarkHold(KeyValueStore<byte[], byte[]> holdsIndexStore) {
    try (KeyValueIterator<byte[], byte[]> it = holdsIndexStore.all()) {
      if (!it.hasNext()) {
        return null;
      }
      return new Instant(StoreKeys.readTimestamp(it.next().key, 0));
    }
  }

  private final @NonNull K key;
  private final byte[] encodedKey;
  private final KeyValueStore<byte[], byte[]> store;
  private final KeyValueStore<byte[], byte[]> holdsIndexStore;

  /**
   * The last namespace a composite key was built for, and the {@code key | namespace} prefix that
   * was built for it. One turn of the windowing runner touches several tags in the same namespace
   * back to back (read the buffer, read the hold, write both), so caching the prefix removes most
   * of the per-access encoding work.
   */
  private @Nullable StateNamespace cachedNamespace;

  private byte @Nullable [] cachedPrefix;

  KafkaStreamsStateInternals(
      @NonNull K key,
      byte[] encodedKey,
      KeyValueStore<byte[], byte[]> store,
      KeyValueStore<byte[], byte[]> holdsIndexStore) {
    this.key = key;
    this.encodedKey = encodedKey;
    this.store = store;
    this.holdsIndexStore = holdsIndexStore;
  }

  @Override
  public Object getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return address.getSpec().bind(address.getId(), new KafkaStreamsStateBinder(namespace, c));
  }

  /**
   * The composite store key for one cell: {@code len|key len|namespace len|tagId}.
   *
   * <p>Built into one exactly-sized array, reusing the cached {@code key | namespace} prefix. This
   * runs on every state access, so it avoids the repeated growth and final copy a stream would do.
   */
  private byte[] compositeKey(StateNamespace namespace, String id) {
    byte[] prefix = prefixFor(namespace);
    byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);
    byte[] compositeKey = new byte[prefix.length + StoreKeys.segmentLength(idBytes)];
    System.arraycopy(prefix, 0, compositeKey, 0, prefix.length);
    StoreKeys.writeSegment(compositeKey, prefix.length, idBytes);
    return compositeKey;
  }

  /** The {@code key | namespace} prefix every cell in {@code namespace} starts with. */
  private byte[] prefixFor(StateNamespace namespace) {
    byte[] cached = cachedPrefix;
    if (cached != null && namespace.equals(cachedNamespace)) {
      return cached;
    }
    byte[] namespaceBytes = namespace.stringKey().getBytes(StandardCharsets.UTF_8);
    byte[] prefix =
        new byte[StoreKeys.segmentLength(encodedKey) + StoreKeys.segmentLength(namespaceBytes)];
    int offset = StoreKeys.writeSegment(prefix, 0, encodedKey);
    StoreKeys.writeSegment(prefix, offset, namespaceBytes);
    cachedNamespace = namespace;
    cachedPrefix = prefix;
    return prefix;
  }

  private class KafkaStreamsStateBinder implements StateBinder {
    private final StateNamespace namespace;
    private final StateContext<?> stateContext;

    private KafkaStreamsStateBinder(StateNamespace namespace, StateContext<?> stateContext) {
      this.namespace = namespace;
      this.stateContext = stateContext;
    }

    @Override
    public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
      return new KafkaStreamsValueState<>(namespace, id, coder);
    }

    @Override
    public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
      return new KafkaStreamsBagState<>(namespace, id, elemCoder);
    }

    @Override
    public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
      throw new UnsupportedOperationException(
          SetState.class.getSimpleName() + " is not supported by the Kafka Streams runner yet");
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        String id,
        StateSpec<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      throw new UnsupportedOperationException(
          MapState.class.getSimpleName() + " is not supported by the Kafka Streams runner yet");
    }

    @Override
    public <KeyT, ValueT> MultimapState<KeyT, ValueT> bindMultimap(
        String id,
        StateSpec<MultimapState<KeyT, ValueT>> spec,
        Coder<KeyT> keyCoder,
        Coder<ValueT> valueCoder) {
      throw new UnsupportedOperationException(
          MultimapState.class.getSimpleName()
              + " is not supported by the Kafka Streams runner yet");
    }

    @Override
    public <T> OrderedListState<T> bindOrderedList(
        String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
      throw new UnsupportedOperationException(
          OrderedListState.class.getSimpleName()
              + " is not supported by the Kafka Streams runner yet");
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new KafkaStreamsCombiningState<>(namespace, id, accumCoder, combineFn);
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
            String id,
            StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
            Coder<AccumT> accumCoder,
            CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return new KafkaStreamsCombiningState<>(
          namespace, id, accumCoder, CombineFnUtil.bindContext(combineFn, stateContext));
    }

    @Override
    public WatermarkHoldState bindWatermark(
        String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
      return new KafkaStreamsWatermarkHoldState(namespace, id, timestampCombiner);
    }
  }

  /** Common read/write/clear against the backing store for one cell. */
  private abstract class AbstractState<T> {
    final StateNamespace namespace;
    final String id;
    final Coder<T> coder;

    AbstractState(StateNamespace namespace, String id, Coder<T> coder) {
      this.namespace = namespace;
      this.id = id;
      this.coder = coder;
    }

    @Nullable
    T readValue() {
      byte[] bytes = store.get(compositeKey(namespace, id));
      if (bytes == null) {
        return null;
      }
      try {
        return CoderUtils.decodeFromByteArray(coder, bytes);
      } catch (CoderException e) {
        throw new RuntimeException("Failed to decode state " + id, e);
      }
    }

    void writeValue(T input) {
      try {
        store.put(compositeKey(namespace, id), CoderUtils.encodeToByteArray(coder, input));
      } catch (CoderException e) {
        throw new RuntimeException("Failed to encode state " + id, e);
      }
    }

    public void clear() {
      store.delete(compositeKey(namespace, id));
    }

    ReadableState<Boolean> isEmptyState() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return store.get(compositeKey(namespace, id)) == null;
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }
  }

  private class KafkaStreamsValueState<T> extends AbstractState<T> implements ValueState<T> {
    KafkaStreamsValueState(StateNamespace namespace, String id, Coder<T> coder) {
      super(namespace, id, coder);
    }

    @Override
    public KafkaStreamsValueState<T> readLater() {
      return this;
    }

    @Override
    public @Nullable T read() {
      return readValue();
    }

    @Override
    public void write(T input) {
      writeValue(input);
    }
  }

  private class KafkaStreamsBagState<T> extends AbstractState<List<T>> implements BagState<T> {
    KafkaStreamsBagState(StateNamespace namespace, String id, Coder<T> elemCoder) {
      super(namespace, id, ListCoder.of(elemCoder));
    }

    @Override
    public KafkaStreamsBagState<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      List<T> value = readValue();
      return value == null ? new ArrayList<>() : value;
    }

    @Override
    public void add(T input) {
      List<T> value = readValue();
      if (value == null) {
        value = new ArrayList<>();
      }
      value.add(input);
      writeValue(value);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }
  }

  private class KafkaStreamsWatermarkHoldState extends AbstractState<Instant>
      implements WatermarkHoldState {
    private final TimestampCombiner timestampCombiner;

    KafkaStreamsWatermarkHoldState(
        StateNamespace namespace, String id, TimestampCombiner timestampCombiner) {
      super(namespace, id, InstantCoder.of());
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public KafkaStreamsWatermarkHoldState readLater() {
      return this;
    }

    // GroupingState.read() is typed non-null, but an empty hold reads back null. Beam's state
    // interfaces are under-annotated here (https://github.com/apache/beam/issues/20497), which is
    // why the Spark and Flink StateInternals suppress nullness for the whole class; this runner
    // narrows the suppression to just this method.
    @Override
    @SuppressWarnings("nullness")
    public Instant read() {
      return readValue();
    }

    @Override
    public void add(Instant outputTime) {
      Instant current = readValue();
      Instant combined =
          current == null ? outputTime : timestampCombiner.combine(current, outputTime);
      writeValue(combined);
      // Mirror the hold into the index so the processor can find the minimum hold across every key
      // and window with one lookup instead of reading all of them.
      if (current != null) {
        holdsIndexStore.delete(holdIndexKey(current));
      }
      holdsIndexStore.put(holdIndexKey(combined), EMPTY_VALUE);
    }

    @Override
    public void clear() {
      Instant current = readValue();
      if (current != null) {
        holdsIndexStore.delete(holdIndexKey(current));
      }
      super.clear();
    }

    /** {@code holdTimestamp | cell}, so the index is ordered by hold time. */
    private byte[] holdIndexKey(Instant hold) {
      byte[] cellKey = compositeKey(namespace, id);
      byte[] indexKey = new byte[StoreKeys.TIMESTAMP_BYTES + cellKey.length];
      int offset = StoreKeys.writeTimestamp(indexKey, 0, hold.getMillis());
      System.arraycopy(cellKey, 0, indexKey, offset, cellKey.length);
      return indexKey;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }
  }

  @SuppressWarnings("TypeParameterShadowing")
  private class KafkaStreamsCombiningState<InputT, AccumT, OutputT> extends AbstractState<AccumT>
      implements CombiningState<InputT, AccumT, OutputT> {
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    KafkaStreamsCombiningState(
        StateNamespace namespace,
        String id,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(namespace, id, accumCoder);
      this.combineFn = combineFn;
    }

    @Override
    public KafkaStreamsCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    // GroupingState.read() is typed non-null but a CombineFn may extract a null output; the same
    // under-annotation as WatermarkHoldState.read() (https://github.com/apache/beam/issues/20497).
    @Override
    @SuppressWarnings("nullness")
    public OutputT read() {
      return combineFn.extractOutput(getAccum());
    }

    @Override
    public void add(InputT input) {
      writeValue(combineFn.addInput(getAccum(), input));
    }

    @Override
    public AccumT getAccum() {
      AccumT accum = readValue();
      return accum == null ? combineFn.createAccumulator() : accum;
    }

    @Override
    public void addAccum(AccumT accum) {
      writeValue(combineFn.mergeAccumulators(Arrays.asList(getAccum(), accum)));
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return isEmptyState();
    }
  }
}
