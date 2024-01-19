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
package org.apache.beam.runners.core;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

/**
 * In-memory implementation of {@link StateInternals}. Used in {@code BatchModeExecutionContext} and
 * for running tests that need state.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class InMemoryStateInternals<K> implements StateInternals {

  public static <K> InMemoryStateInternals<K> forKey(@Nullable K key) {
    return new InMemoryStateInternals<>(key);
  }

  private final @Nullable K key;

  protected InMemoryStateInternals(@Nullable K key) {
    this.key = key;
  }

  @Override
  public @Nullable K getKey() {
    return key;
  }

  /**
   * Interface common to all in-memory state cells. Includes ability to see whether a cell has been
   * cleared and the ability to create a clone of the contents.
   */
  public interface InMemoryState<T extends InMemoryState<T>> {
    boolean isCleared();

    T copy();
  }

  protected final StateTable inMemoryState =
      new StateTable() {
        @Override
        protected StateBinder binderForNamespace(StateNamespace namespace, StateContext<?> c) {
          return new InMemoryStateBinder(c);
        }
      };

  public void clear() {
    inMemoryState.clear();
  }

  /**
   * Return true if the given state is empty. This is used by the test framework to make sure that
   * the state has been properly cleaned up.
   */
  protected boolean isEmptyForTesting(State state) {
    return ((InMemoryState<?>) state).isCleared();
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, final StateContext<?> c) {
    return inMemoryState.get(namespace, address, c);
  }

  /** A {@link StateBinder} that returns In Memory {@link State} objects. */
  public static class InMemoryStateBinder implements StateBinder {
    private final StateContext<?> c;

    public InMemoryStateBinder(StateContext<?> c) {
      this.c = c;
    }

    @Override
    public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
      return new InMemoryValue<>(coder);
    }

    @Override
    public <T> BagState<T> bindBag(final StateTag<BagState<T>> address, Coder<T> elemCoder) {
      return new InMemoryBag<>(elemCoder);
    }

    @Override
    public <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder) {
      return new InMemorySet<>(elemCoder);
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        StateTag<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      return new InMemoryMap<>(mapKeyCoder, mapValueCoder);
    }

    @Override
    public <KeyT, ValueT> MultimapState<KeyT, ValueT> bindMultimap(
        StateTag<MultimapState<KeyT, ValueT>> spec,
        Coder<KeyT> keyCoder,
        Coder<ValueT> valueCoder) {
      return new InMemoryMultimap<>(keyCoder, valueCoder);
    }

    @Override
    public <T> OrderedListState<T> bindOrderedList(
        StateTag<OrderedListState<T>> spec, Coder<T> elemCoder) {
      return new InMemoryOrderedList<>(elemCoder);
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        final CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new InMemoryCombiningState<>(combineFn, accumCoder);
    }

    @Override
    public WatermarkHoldState bindWatermark(
        StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
      return new InMemoryWatermarkHold(timestampCombiner);
    }

    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
            StateTag<CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  /** An {@link InMemoryState} implementation of {@link ValueState}. */
  public static final class InMemoryValue<T>
      implements ValueState<T>, InMemoryState<InMemoryValue<T>> {
    private final Coder<T> coder;

    private boolean isCleared = true;
    private @Nullable T value = null;

    public InMemoryValue(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this Value.
      value = null;
      isCleared = true;
    }

    @Override
    public InMemoryValue<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      return value;
    }

    @Override
    public void write(T input) {
      isCleared = false;
      this.value = input;
    }

    @Override
    public InMemoryValue<T> copy() {
      InMemoryValue<T> that = new InMemoryValue<>(coder);
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.value = uncheckedClone(coder, this.value);
      }
      return that;
    }

    @Override
    public boolean isCleared() {
      return isCleared;
    }
  }

  /** An {@link InMemoryState} implementation of {@link WatermarkHoldState}. */
  public static final class InMemoryWatermarkHold<W extends BoundedWindow>
      implements WatermarkHoldState, InMemoryState<InMemoryWatermarkHold<W>> {

    private final TimestampCombiner timestampCombiner;

    private @Nullable Instant combinedHold = null;

    public InMemoryWatermarkHold(TimestampCombiner timestampCombiner) {
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public InMemoryWatermarkHold readLater() {
      return this;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this WatermarkBagInternal.
      combinedHold = null;
    }

    @Override
    public Instant read() {
      return combinedHold;
    }

    @Override
    public void add(Instant outputTime) {
      combinedHold =
          combinedHold == null ? outputTime : timestampCombiner.combine(combinedHold, outputTime);
    }

    @Override
    public boolean isCleared() {
      return combinedHold == null;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return combinedHold == null;
        }
      };
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }

    @Override
    public String toString() {
      return Objects.toString(combinedHold);
    }

    @Override
    public InMemoryWatermarkHold<W> copy() {
      InMemoryWatermarkHold<W> that = new InMemoryWatermarkHold<>(timestampCombiner);
      that.combinedHold = this.combinedHold;
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link CombiningState}. */
  public static final class InMemoryCombiningState<InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT>,
          InMemoryState<InMemoryCombiningState<InputT, AccumT, OutputT>> {
    private final CombineFn<InputT, AccumT, OutputT> combineFn;
    private final Coder<AccumT> accumCoder;
    private boolean isCleared = true;
    private AccumT accum;

    public InMemoryCombiningState(
        CombineFn<InputT, AccumT, OutputT> combineFn, Coder<AccumT> accumCoder) {
      this.combineFn = combineFn;
      accum = combineFn.createAccumulator();
      this.accumCoder = accumCoder;
    }

    @Override
    public InMemoryCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this CombiningValue.
      accum = combineFn.createAccumulator();
      isCleared = true;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(
          combineFn.mergeAccumulators(Arrays.asList(combineFn.createAccumulator(), accum)));
    }

    @Override
    public void add(InputT input) {
      isCleared = false;
      accum = combineFn.addInput(accum, input);
    }

    @Override
    public AccumT getAccum() {
      return accum;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return isCleared;
        }
      };
    }

    @Override
    public void addAccum(AccumT accum) {
      isCleared = false;
      this.accum = combineFn.mergeAccumulators(Arrays.asList(this.accum, accum));
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public boolean isCleared() {
      return isCleared;
    }

    @Override
    public InMemoryCombiningState<InputT, AccumT, OutputT> copy() {
      InMemoryCombiningState<InputT, AccumT, OutputT> that =
          new InMemoryCombiningState<>(combineFn, accumCoder);
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.addAccum(uncheckedClone(accumCoder, accum));
      }
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link BagState}. */
  public static final class InMemoryBag<T> implements BagState<T>, InMemoryState<InMemoryBag<T>> {
    private final Coder<T> elemCoder;
    private List<T> contents = new ArrayList<>();

    public InMemoryBag(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this Bag.
      // The result of get/read below must be stable for the lifetime of the bundle within which it
      // was generated. In batch and direct runners the bundle lifetime can be
      // greater than the window lifetime, in which case this method can be called while
      // the result is still in use. We protect against this by hot-swapping instead of
      // clearing the contents.
      contents = new ArrayList<>();
    }

    @Override
    public InMemoryBag<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      return Iterables.limit(contents, contents.size());
    }

    @Override
    public void add(T input) {
      contents.add(input);
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return contents.isEmpty();
        }
      };
    }

    @Override
    public InMemoryBag<T> copy() {
      InMemoryBag<T> that = new InMemoryBag<>(elemCoder);
      for (T elem : this.contents) {
        that.contents.add(uncheckedClone(elemCoder, elem));
      }
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link MultimapState}. */
  public static final class InMemoryMultimap<K, V>
      implements MultimapState<K, V>, InMemoryState<InMemoryMultimap<K, V>> {
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private Multimap<Object, V> contents = ArrayListMultimap.create();
    private Map<Object, K> structuralKeysMapping = Maps.newHashMap();

    public InMemoryMultimap(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public void clear() {
      contents = ArrayListMultimap.create();
      structuralKeysMapping = Maps.newHashMap();
    }

    @Override
    public void put(K key, V value) {
      Object structuralKey = keyCoder.structuralValue(key);
      structuralKeysMapping.put(structuralKey, key);
      contents.put(structuralKey, value);
    }

    @Override
    public void remove(K key) {
      Object structuralKey = keyCoder.structuralValue(key);
      structuralKeysMapping.remove(structuralKey);
      contents.removeAll(structuralKey);
    }

    @Override
    public ReadableState<Iterable<V>> get(K key) {
      return new ReadableState<Iterable<V>>() {
        @Override
        public Iterable<V> read() {
          return ImmutableList.copyOf(contents.get(keyCoder.structuralValue(key)));
        }

        @Override
        public ReadableState<Iterable<V>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterable<K>> keys() {
      return new ReadableState<Iterable<K>>() {
        @Override
        public Iterable<K> read() {
          return ImmutableList.copyOf(structuralKeysMapping.values());
        }

        @Override
        public ReadableState<Iterable<K>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
      return new ReadableState<Iterable<Map.Entry<K, V>>>() {
        @Override
        public Iterable<Map.Entry<K, V>> read() {
          List<Map.Entry<K, V>> result =
              Lists.newArrayListWithExpectedSize(contents.entries().size());
          for (Map.Entry<Object, V> entry : contents.entries()) {
            result.add(
                new AbstractMap.SimpleEntry<>(
                    structuralKeysMapping.get(entry.getKey()), entry.getValue()));
          }
          return Collections.unmodifiableList(result);
        }

        @Override
        public ReadableState<Iterable<Map.Entry<K, V>>> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Boolean> containsKey(K key) {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return structuralKeysMapping.containsKey(keyCoder.structuralValue(key));
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return contents.isEmpty();
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public InMemoryMultimap<K, V> copy() {
      InMemoryMultimap<K, V> that = new InMemoryMultimap<>(keyCoder, valueCoder);
      for (K key : this.structuralKeysMapping.values()) {
        // Make a copy of structuralKey as well
        Object structuralKey = keyCoder.structuralValue(key);
        that.structuralKeysMapping.put(structuralKey, uncheckedClone(keyCoder, key));
        for (V value : this.contents.get(structuralKey)) {
          that.contents.put(structuralKey, uncheckedClone(valueCoder, value));
        }
      }
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link OrderedListState}. */
  public static final class InMemoryOrderedList<T>
      implements OrderedListState<T>, InMemoryState<InMemoryOrderedList<T>> {
    private final Coder<T> elemCoder;
    private NavigableMap<Instant, Collection<T>> contents = Maps.newTreeMap();

    public InMemoryOrderedList(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state, since
      // other users may already have a handle on this list.
      // The result of get/read below must be stable for the lifetime of the bundle within which it
      // was generated. In batch and direct runners the bundle lifetime can be
      // greater than the window lifetime, in which case this method can be called while
      // the result is still in use. We protect against this by hot-swapping instead of
      // clearing the contents.
      contents = Maps.newTreeMap();
    }

    @Override
    public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
      contents.subMap(minTimestamp, true, limitTimestamp, false).clear();
    }

    @Override
    public InMemoryOrderedList<T> readLater() {
      return this;
    }

    @Override
    public OrderedListState<T> readRangeLater(Instant minTimestamp, Instant limitTimestamp) {
      return this;
    }

    @Override
    public Iterable<TimestampedValue<T>> read() {
      return readRange(Instant.ofEpochMilli(Long.MIN_VALUE), Instant.ofEpochMilli(Long.MAX_VALUE));
    }

    @Override
    public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
      return contents.subMap(minTimestamp, true, limitTimestamp, false).entrySet().stream()
          .flatMap(e -> e.getValue().stream().map(v -> TimestampedValue.of(v, e.getKey())))
          .collect(Collectors.toList());
    }

    @Override
    public void add(TimestampedValue<T> input) {
      contents
          .computeIfAbsent(input.getTimestamp(), x -> Lists.newArrayList())
          .add(input.getValue());
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return contents.isEmpty();
        }
      };
    }

    @Override
    public InMemoryOrderedList<T> copy() {
      InMemoryOrderedList<T> that = new InMemoryOrderedList<>(elemCoder);
      this.contents.entrySet().stream()
          .flatMap(e -> e.getValue().stream().map(v -> TimestampedValue.of(v, e.getKey())))
          .forEach(that::add);
      return that;
    }
  }

  /** An {@link InMemoryState} implementation of {@link SetState}. */
  public static final class InMemorySet<T> implements SetState<T>, InMemoryState<InMemorySet<T>> {
    private final Coder<T> elemCoder;
    private Set<T> contents = new HashSet<>();

    public InMemorySet(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      contents = new HashSet<>();
    }

    @Override
    public ReadableState<Boolean> contains(T t) {
      return ReadableStates.immediate(contents.contains(t));
    }

    @Override
    public ReadableState<Boolean> addIfAbsent(T t) {
      boolean alreadyContained = contents.contains(t);
      contents.add(t);
      return ReadableStates.immediate(!alreadyContained);
    }

    @Override
    public void remove(T t) {
      contents.remove(t);
    }

    @Override
    public InMemorySet<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      return ImmutableSet.copyOf(contents);
    }

    @Override
    public void add(T input) {
      contents.add(input);
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return contents.isEmpty();
        }
      };
    }

    @Override
    public InMemorySet<T> copy() {
      InMemorySet<T> that = new InMemorySet<>(elemCoder);
      for (T elem : this.contents) {
        that.contents.add(uncheckedClone(elemCoder, elem));
      }
      return that;
    }
  }

  private static class CollectionViewState<T> implements ReadableState<Iterable<T>> {
    private final Collection<T> collection;

    private CollectionViewState(Collection<T> collection) {
      this.collection = collection;
    }

    public static <T> CollectionViewState<T> of(Collection<T> collection) {
      return new CollectionViewState<>(collection);
    }

    @Override
    public Iterable<T> read() {
      return ImmutableList.copyOf(collection);
    }

    @Override
    public ReadableState<Iterable<T>> readLater() {
      return this;
    }
  }

  /** An {@link InMemoryState} implementation of {@link MapState}. */
  public static final class InMemoryMap<K, V>
      implements MapState<K, V>, InMemoryState<InMemoryMap<K, V>> {
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    private Map<K, V> contents = new HashMap<>();

    public InMemoryMap(Coder<K> keyCoder, Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public void clear() {
      contents = new HashMap<>();
    }

    @Override
    public ReadableState<V> get(K key) {
      return getOrDefault(key, null);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<V> getOrDefault(
        K key, @Nullable V defaultValue) {
      return new ReadableState<V>() {
        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable V read() {
          return contents.getOrDefault(key, defaultValue);
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<V> readLater() {
          return this;
        }
      };
    }

    @Override
    public void put(K key, V value) {
      contents.put(key, value);
    }

    @Override
    public ReadableState<V> computeIfAbsent(
        K key, Function<? super K, ? extends V> mappingFunction) {
      V v = contents.get(key);
      if (v == null) {
        v = contents.put(key, mappingFunction.apply(key));
      }

      return ReadableStates.immediate(v);
    }

    @Override
    public void remove(K key) {
      contents.remove(key);
    }

    @Override
    public ReadableState<Iterable<K>> keys() {
      return CollectionViewState.of(contents.keySet());
    }

    @Override
    public ReadableState<Iterable<V>> values() {
      return CollectionViewState.of(contents.values());
    }

    @Override
    public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
      return CollectionViewState.of(contents.entrySet());
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Boolean>
        isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable Boolean read() {
          return contents.isEmpty();
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public InMemoryMap<K, V> copy() {
      InMemoryMap<K, V> that = new InMemoryMap<>(keyCoder, valueCoder);
      for (Map.Entry<K, V> entry : this.contents.entrySet()) {
        that.contents.put(
            uncheckedClone(keyCoder, entry.getKey()), uncheckedClone(valueCoder, entry.getValue()));
      }
      that.contents.putAll(this.contents);
      return that;
    }
  }

  /** Like {@link CoderUtils#clone} but without a checked exception. */
  private static <T> T uncheckedClone(Coder<T> coder, T value) {
    try {
      return CoderUtils.clone(coder, value);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }
}
