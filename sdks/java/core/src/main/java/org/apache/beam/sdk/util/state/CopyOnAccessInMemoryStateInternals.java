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
package org.apache.beam.sdk.util.state;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.state.InMemoryStateInternals.InMemoryState;
import org.apache.beam.sdk.util.state.StateTag.StateBinder;
import org.joda.time.Instant;

/**
 * {@link StateInternals} built on top of an underlying {@link StateTable} that contains instances
 * of {@link InMemoryState}. Whenever state that exists in the underlying {@link StateTable} is
 * accessed, an independent copy will be created within this table.
 */
public class CopyOnAccessInMemoryStateInternals<K> implements StateInternals<K> {
  private final K key;
  private final CopyOnAccessInMemoryStateTable<K> table;

  /**
   * Creates a new {@link CopyOnAccessInMemoryStateInternals} with the underlying (possibly null)
   * StateInternals.
   */
  public static <K> CopyOnAccessInMemoryStateInternals<K> withUnderlying(
      K key, @Nullable CopyOnAccessInMemoryStateInternals<K> underlying) {
    return new CopyOnAccessInMemoryStateInternals<K>(key, underlying);
  }

  private CopyOnAccessInMemoryStateInternals(
      K key, CopyOnAccessInMemoryStateInternals<K> underlying) {
    this.key = key;
    table =
        new CopyOnAccessInMemoryStateTable<K>(key, underlying == null ? null : underlying.table);
  }

  /**
   * Ensures this {@link CopyOnAccessInMemoryStateInternals} is complete. Other copies of state for
   * the same Step and Key may be discarded after invoking this method.
   *
   * <p>For each {@link StateNamespace}, for each {@link StateTag address} in that namespace that
   * has not been bound in this {@link CopyOnAccessInMemoryStateInternals}, put a reference to that
   * state within this {@link StateInternals}.
   *
   * <p>Additionally, stores the {@link WatermarkHoldState} with the earliest time bound in the
   * state table after the commit is completed, enabling calls to
   * {@link #getEarliestWatermarkHold()}.
   *
   * @return this table
   */
  public CopyOnAccessInMemoryStateInternals<K> commit() {
    table.commit();
    return this;
  }

  /**
   * Gets the earliest Watermark Hold present in this table.
   *
   * <p>Must be called after this state has been committed. Will throw an
   * {@link IllegalStateException} if the state has not been committed.
   */
  public Instant getEarliestWatermarkHold() {
    // After commit, the watermark hold is always present, but may be
    // BoundedWindow#TIMESTAMP_MAX_VALUE if there is no hold set.
    checkState(
        table.earliestWatermarkHold.isPresent(),
        "Can't get the earliest watermark hold in a %s before it is committed",
        getClass().getSimpleName());
    return table.earliestWatermarkHold.get();
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address) {
    return state(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<? super K, T> address, StateContext<?> c) {
    return table.get(namespace, address, c);
  }

  @Override
  public K getKey() {
    return key;
  }

  public boolean isEmpty() {
    return Iterables.isEmpty(table.values());
  }

  /**
   * A {@link StateTable} that, when a value is retrieved with
   * {@link StateTable#get(StateNamespace, StateTag, StateContext)}, first attempts to obtain a
   * copy of existing {@link State} from an underlying {@link StateTable}.
   */
  private static class CopyOnAccessInMemoryStateTable<K> extends StateTable<K> {
    private final K key;
    private Optional<StateTable<K>> underlying;

    /**
     * The StateBinderFactory currently in use by this {@link CopyOnAccessInMemoryStateTable}.
     *
     * <p>There are three {@link StateBinderFactory} implementations used by the {@link
     * CopyOnAccessInMemoryStateTable}.
     * <ul>
     *   <li>The default {@link StateBinderFactory} is a {@link CopyOnBindBinderFactory}, allowing
     *       the table to copy any existing {@link State} values to this {@link StateTable} from the
     *       underlying table when accessed, at which point mutations will not be visible to the
     *       underlying table - effectively a "Copy by Value" binder.</li>
     *   <li>During the execution of the {@link #commit()} method, this is a
     *       {@link ReadThroughBinderFactory}, which copies the references to the existing
     *       {@link State} objects to this {@link StateTable}.</li>
     *   <li>After the execution of the {@link #commit()} method, this is an
     *       instance of {@link InMemoryStateBinderFactory}, which constructs new instances of state
     *       when a {@link StateTag} is bound.</li>
     * </ul>
     */
    private StateBinderFactory<K> binderFactory;

    /**
     * The earliest watermark hold in this table.
     */
    private Optional<Instant> earliestWatermarkHold;

    public CopyOnAccessInMemoryStateTable(K key, StateTable<K> underlying) {
      this.key = key;
      this.underlying = Optional.fromNullable(underlying);
      binderFactory = new CopyOnBindBinderFactory<>(key, this.underlying);
      earliestWatermarkHold = Optional.absent();
    }

    /**
     * Copies all values in the underlying table to this table, then discards the underlying table.
     *
     * <p>If there is an underlying table, this replaces the existing
     * {@link CopyOnBindBinderFactory} with a {@link ReadThroughBinderFactory}, then reads all of
     * the values in the existing table, binding the state values to this table. The old StateTable
     * should be discarded after the call to {@link #commit()}.
     *
     * <p>After copying all of the existing values, replace the binder factory with an instance of
     * {@link InMemoryStateBinderFactory} to construct new values, since all existing values
     * are bound in this {@link StateTable table} and this table represents the canonical state.
     */
    private void commit() {
      Instant earliestHold = getEarliestWatermarkHold();
      if (underlying.isPresent()) {
        ReadThroughBinderFactory<K> readThroughBinder =
            new ReadThroughBinderFactory<>(underlying.get());
        binderFactory = readThroughBinder;
        Instant earliestUnderlyingHold = readThroughBinder.readThroughAndGetEarliestHold(this);
        if (earliestUnderlyingHold.isBefore(earliestHold)) {
          earliestHold = earliestUnderlyingHold;
        }
      }
      earliestWatermarkHold = Optional.of(earliestHold);
      clearEmpty();
      binderFactory = new InMemoryStateBinderFactory<>(key);
      underlying = Optional.absent();
    }

    /**
     * Get the earliest watermark hold in this table. Ignores the contents of any underlying table.
     */
    private Instant getEarliestWatermarkHold() {
      Instant earliest = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (State existingState : this.values()) {
        if (existingState instanceof WatermarkHoldState) {
          Instant hold = ((WatermarkHoldState<?>) existingState).read();
          if (hold != null && hold.isBefore(earliest)) {
            earliest = hold;
          }
        }
      }
      return earliest;
    }

    /**
     * Clear all empty {@link StateNamespace StateNamespaces} from this table. If all states are
     * empty, clear the entire table.
     *
     * <p>Because {@link InMemoryState} is not removed from the {@link StateTable} after it is
     * cleared, in case contents are modified after being cleared, the table must be explicitly
     * checked to ensure that it contains state and removed if not (otherwise we may never use
     * the table again).
     */
    private void clearEmpty() {
      Collection<StateNamespace> emptyNamespaces = new HashSet<>(this.getNamespacesInUse());
      for (StateNamespace namespace : this.getNamespacesInUse()) {
        for (State existingState : this.getTagsInUse(namespace).values()) {
          if (!((InMemoryState<?>) existingState).isCleared()) {
            emptyNamespaces.remove(namespace);
            break;
          }
        }
      }
      for (StateNamespace empty : emptyNamespaces) {
        this.clearNamespace(empty);
      }
    }

    @Override
    protected StateBinder<K> binderForNamespace(final StateNamespace namespace, StateContext<?> c) {
      return binderFactory.forNamespace(namespace, c);
    }

    private interface StateBinderFactory<K> {
      StateBinder<K> forNamespace(StateNamespace namespace, StateContext<?> c);
    }

    /**
     * {@link StateBinderFactory} that creates a copy of any existing state when the state is bound.
     */
    private static class CopyOnBindBinderFactory<K> implements StateBinderFactory<K> {
      private final K key;
      private final Optional<StateTable<K>> underlying;

      public CopyOnBindBinderFactory(K key, Optional<StateTable<K>> underlying) {
        this.key = key;
        this.underlying = underlying;
      }

      private boolean containedInUnderlying(StateNamespace namespace, StateTag<? super K, ?> tag) {
        return underlying.isPresent() && underlying.get().isNamespaceInUse(namespace)
            && underlying.get().getTagsInUse(namespace).containsKey(tag);
      }

      @Override
      public StateBinder<K> forNamespace(final StateNamespace namespace, final StateContext<?> c) {
        return new StateBinder<K>() {
          @Override
          public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
              StateTag<? super K, WatermarkHoldState<W>> address,
              OutputTimeFn<? super W> outputTimeFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends WatermarkHoldState<W>> existingState =
                  (InMemoryStateInternals.InMemoryState<? extends WatermarkHoldState<W>>)
                  underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryWatermarkHold<>(
                  outputTimeFn);
            }
          }

          @Override
          public <T> ValueState<T> bindValue(
              StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends ValueState<T>> existingState =
                  (InMemoryStateInternals.InMemoryState<? extends ValueState<T>>)
                  underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryValue<>();
            }
          }

          @Override
          public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
              bindCombiningValue(
                  StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends AccumulatorCombiningState<InputT, AccumT, OutputT>>
                  existingState = (
                      InMemoryStateInternals
                          .InMemoryState<? extends AccumulatorCombiningState<InputT, AccumT,
                          OutputT>>) underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryCombiningValue<>(
                  key, combineFn.asKeyedFn());
            }
          }

          @Override
          public <T> BagState<T> bindBag(
              StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends BagState<T>> existingState =
                  (InMemoryStateInternals.InMemoryState<? extends BagState<T>>)
                  underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryBag<>();
            }
          }

          @Override
          public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
              bindKeyedCombiningValue(
                  StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends AccumulatorCombiningState<InputT, AccumT, OutputT>>
                  existingState = (
                      InMemoryStateInternals
                          .InMemoryState<? extends AccumulatorCombiningState<InputT, AccumT,
                          OutputT>>) underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryCombiningValue<>(key, combineFn);
            }
          }

          @Override
          public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
          bindKeyedCombiningValueWithContext(
                  StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
            return bindKeyedCombiningValue(
                address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
          }
        };
      }
    }

    /**
     * {@link StateBinderFactory} that reads directly from the underlying table. Used during calls
     * to {@link CopyOnAccessInMemoryStateTable#commit()} to read all values from
     * the underlying table.
     */
    private static class ReadThroughBinderFactory<K> implements StateBinderFactory<K> {
      private final StateTable<K> underlying;

      public ReadThroughBinderFactory(StateTable<K> underlying) {
        this.underlying = underlying;
      }

      public Instant readThroughAndGetEarliestHold(StateTable<K> readTo) {
        Instant earliestHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
        for (StateNamespace namespace : underlying.getNamespacesInUse()) {
          for (Map.Entry<StateTag<? super K, ?>, ? extends State> existingState :
              underlying.getTagsInUse(namespace).entrySet()) {
            if (!((InMemoryState<?>) existingState.getValue()).isCleared()) {
              // Only read through non-cleared values to ensure that completed windows are
              // eventually discarded, and remember the earliest watermark hold from among those
              // values.
              State state =
                  readTo.get(namespace, existingState.getKey(), StateContexts.nullContext());
              if (state instanceof WatermarkHoldState) {
                Instant hold = ((WatermarkHoldState<?>) state).read();
                if (hold != null && hold.isBefore(earliestHold)) {
                  earliestHold = hold;
                }
              }
            }
          }
        }
        return earliestHold;
      }

      @Override
      public StateBinder<K> forNamespace(final StateNamespace namespace, final StateContext<?> c) {
        return new StateBinder<K>() {
          @Override
          public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
              StateTag<? super K, WatermarkHoldState<W>> address,
              OutputTimeFn<? super W> outputTimeFn) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <T> ValueState<T> bindValue(
              StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
              bindCombiningValue(
                  StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <T> BagState<T> bindBag(
              StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
              bindKeyedCombiningValue(
                  StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
          bindKeyedCombiningValueWithContext(
                  StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
            return bindKeyedCombiningValue(
                address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
          }
        };
      }
    }

    private static class InMemoryStateBinderFactory<K> implements StateBinderFactory<K> {
      private final K key;

      public InMemoryStateBinderFactory(K key) {
        this.key = key;
      }

      @Override
      public StateBinder<K> forNamespace(StateNamespace namespace, StateContext<?> c) {
        return new InMemoryStateInternals.InMemoryStateBinder<>(key, c);
      }
    }
  }
}
