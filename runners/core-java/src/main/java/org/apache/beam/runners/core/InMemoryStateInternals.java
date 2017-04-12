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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.MapState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.ReadableStates;
import org.apache.beam.sdk.util.state.SetState;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateContext;
import org.apache.beam.sdk.util.state.StateContexts;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.joda.time.Instant;

/**
 * In-memory implementation of {@link StateInternals}. Used in {@code BatchModeExecutionContext}
 * and for running tests that need state.
 */
@Experimental(Kind.STATE)
public class InMemoryStateInternals<K> implements StateInternals<K> {

  public static <K> InMemoryStateInternals<K> forKey(K key) {
    return new InMemoryStateInternals<>(key);
  }

  private final K key;

  protected InMemoryStateInternals(K key) {
    this.key = key;
  }

  @Override
  public K getKey() {
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

  protected final StateTable<K> inMemoryState = new StateTable<K>() {
    @Override
    protected StateBinder<K> binderForNamespace(StateNamespace namespace, StateContext<?> c) {
      return new InMemoryStateBinder<K>(key, c);
    }
  };

  public void clear() {
    inMemoryState.clear();
  }

  /**
   * Return true if the given state is empty. This is used by the test framework to make sure
   * that the state has been properly cleaned up.
   */
  protected boolean isEmptyForTesting(State state) {
    return ((InMemoryState<?>) state).isCleared();
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address) {
    return inMemoryState.get(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<? super K, T> address, final StateContext<?> c) {
    return inMemoryState.get(namespace, address, c);
  }

  /**
   * A {@link StateBinder} that returns In Memory {@link State} objects.
   */
  public static class InMemoryStateBinder<K> implements StateBinder<K> {
    private final K key;
    private final StateContext<?> c;

    public InMemoryStateBinder(K key, StateContext<?> c) {
      this.key = key;
      this.c = c;
    }

    @Override
    public <T> ValueState<T> bindValue(
        StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
      return new InMemoryValue<T>();
    }

    @Override
    public <T> BagState<T> bindBag(
        final StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
      return new InMemoryBag<T>();
    }

    @Override
    public <T> SetState<T> bindSet(StateTag<? super K, SetState<T>> spec, Coder<T> elemCoder) {
      return new InMemorySet<>();
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        StateTag<? super K, MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder, Coder<ValueT> mapValueCoder) {
      return new InMemoryMap<>();
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
        bindCombiningValue(
            StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            final CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new InMemoryCombiningState<K, InputT, AccumT, OutputT>(key, combineFn.<K>asKeyedFn());
    }

    @Override
    public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
        StateTag<? super K, WatermarkHoldState<W>> address,
        OutputTimeFn<? super W> outputTimeFn) {
      return new InMemoryWatermarkHold<W>(outputTimeFn);
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
        bindKeyedCombiningValue(
            StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
      return new InMemoryCombiningState<K, InputT, AccumT, OutputT>(key, combineFn);
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
        bindKeyedCombiningValueWithContext(
            StateTag<? super K, CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
      return bindKeyedCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link ValueState}.
   */
  public static final class InMemoryValue<T>
      implements ValueState<T>, InMemoryState<InMemoryValue<T>> {
    private boolean isCleared = true;
    private T value = null;

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
      InMemoryValue<T> that = new InMemoryValue<>();
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.value = this.value;
      }
      return that;
    }

    @Override
    public boolean isCleared() {
      return isCleared;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link WatermarkHoldState}.
   */
  public static final class InMemoryWatermarkHold<W extends BoundedWindow>
      implements WatermarkHoldState<W>, InMemoryState<InMemoryWatermarkHold<W>> {

    private final OutputTimeFn<? super W> outputTimeFn;

    @Nullable
    private Instant combinedHold = null;

    public InMemoryWatermarkHold(OutputTimeFn<? super W> outputTimeFn) {
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public InMemoryWatermarkHold<W> readLater() {
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
      combinedHold = combinedHold == null ? outputTime
          : outputTimeFn.combine(combinedHold, outputTime);
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
    public OutputTimeFn<? super W> getOutputTimeFn() {
      return outputTimeFn;
    }

    @Override
    public String toString() {
      return Objects.toString(combinedHold);
    }

    @Override
    public InMemoryWatermarkHold<W> copy() {
      InMemoryWatermarkHold<W> that =
          new InMemoryWatermarkHold<>(outputTimeFn);
      that.combinedHold = this.combinedHold;
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link CombiningState}.
   */
  public static final class InMemoryCombiningState<K, InputT, AccumT, OutputT>
      implements CombiningState<InputT, AccumT, OutputT>,
          InMemoryState<InMemoryCombiningState<K, InputT, AccumT, OutputT>> {
    private final K key;
    private boolean isCleared = true;
    private final KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn;
    private AccumT accum;

    public InMemoryCombiningState(
        K key, KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
      this.key = key;
      this.combineFn = combineFn;
      accum = combineFn.createAccumulator(key);
    }

    @Override
    public InMemoryCombiningState<K, InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this CombiningValue.
      accum = combineFn.createAccumulator(key);
      isCleared = true;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(key, accum);
    }

    @Override
    public void add(InputT input) {
      isCleared = false;
      accum = combineFn.addInput(key, accum, input);
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
      this.accum = combineFn.mergeAccumulators(key, Arrays.asList(this.accum, accum));
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(key, accumulators);
    }

    @Override
    public boolean isCleared() {
      return isCleared;
    }

    @Override
    public InMemoryCombiningState<K, InputT, AccumT, OutputT> copy() {
      InMemoryCombiningState<K, InputT, AccumT, OutputT> that =
          new InMemoryCombiningState<>(key, combineFn);
      if (!this.isCleared) {
        that.isCleared = this.isCleared;
        that.addAccum(accum);
      }
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link BagState}.
   */
  public static final class InMemoryBag<T> implements BagState<T>, InMemoryState<InMemoryBag<T>> {
    private List<T> contents = new ArrayList<>();

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
      return contents;
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
      InMemoryBag<T> that = new InMemoryBag<>();
      that.contents.addAll(this.contents);
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link SetState}.
   */
  public static final class InMemorySet<T> implements SetState<T>, InMemoryState<InMemorySet<T>> {
    private Set<T> contents = new HashSet<>();

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
      return contents;
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
      InMemorySet<T> that = new InMemorySet<>();
      that.contents.addAll(this.contents);
      return that;
    }
  }

  /**
   * An {@link InMemoryState} implementation of {@link MapState}.
   */
  public static final class InMemoryMap<K, V> implements
      MapState<K, V>, InMemoryState<InMemoryMap<K, V>> {
    private Map<K, V> contents = new HashMap<>();

    @Override
    public void clear() {
      contents = new HashMap<>();
    }

    @Override
    public ReadableState<V> get(K key) {
      return ReadableStates.immediate(contents.get(key));
    }

    @Override
    public void put(K key, V value) {
      contents.put(key, value);
    }

    @Override
    public ReadableState<V> putIfAbsent(K key, V value) {
      V v = contents.get(key);
      if (v == null) {
        v = contents.put(key, value);
      }

      return ReadableStates.immediate(v);
    }

    @Override
    public void remove(K key) {
      contents.remove(key);
    }

    @Override
    public ReadableState<Iterable<K>> keys() {
      return ReadableStates.immediate((Iterable<K>) contents.keySet());
    }

    @Override
    public ReadableState<Iterable<V>> values() {
      return ReadableStates.immediate((Iterable<V>) contents.values());
    }

    @Override
    public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
      return ReadableStates.immediate((Iterable<Map.Entry<K, V>>) contents.entrySet());
    }

    @Override
    public boolean isCleared() {
      return contents.isEmpty();
    }

    @Override
    public InMemoryMap<K, V> copy() {
      InMemoryMap<K, V> that = new InMemoryMap<>();
      that.contents.putAll(this.contents);
      return that;
    }
  }
}
