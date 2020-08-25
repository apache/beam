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
package org.apache.beam.runners.direct;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryBag;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryCombiningState;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryMap;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryOrderedList;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemorySet;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryState;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryStateBinder;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryValue;
import org.apache.beam.runners.core.InMemoryStateInternals.InMemoryWatermarkHold;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@link StateInternals} built on top of an underlying {@link StateTable} that contains instances
 * of {@link InMemoryState}. Whenever state that exists in the underlying {@link StateTable} is
 * accessed, an independent copy will be created within this table.
 */
class CopyOnAccessInMemoryStateInternals<K> implements StateInternals {
  private final CopyOnAccessInMemoryStateTable table;

  private K key;

  /**
   * Creates a new {@link CopyOnAccessInMemoryStateInternals} with the underlying (possibly null)
   * StateInternals.
   */
  public static <K> CopyOnAccessInMemoryStateInternals withUnderlying(
      K key, @Nullable CopyOnAccessInMemoryStateInternals underlying) {
    return new CopyOnAccessInMemoryStateInternals<>(key, underlying);
  }

  private CopyOnAccessInMemoryStateInternals(K key, CopyOnAccessInMemoryStateInternals underlying) {
    this.key = key;
    table = new CopyOnAccessInMemoryStateTable(underlying == null ? null : underlying.table);
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
   * state table after the commit is completed, enabling calls to {@link
   * #getEarliestWatermarkHold()}.
   *
   * @return this table
   */
  public CopyOnAccessInMemoryStateInternals commit() {
    table.commit();
    return this;
  }

  /**
   * Gets the earliest Watermark Hold present in this table.
   *
   * <p>Must be called after this state has been committed. Will throw an {@link
   * IllegalStateException} if the state has not been committed.
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
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return table.get(namespace, address, c);
  }

  @Override
  public Object getKey() {
    return key;
  }

  public boolean isEmpty() {
    return Iterables.isEmpty(table.values());
  }

  /**
   * A {@link StateTable} that, when a value is retrieved with {@link StateTable#get(StateNamespace,
   * StateTag, StateContext)}, first attempts to obtain a copy of existing {@link State} from an
   * underlying {@link StateTable}.
   */
  private static class CopyOnAccessInMemoryStateTable extends StateTable {
    private Optional<StateTable> underlying;

    /**
     * The StateBinderFactory currently in use by this {@link CopyOnAccessInMemoryStateTable}.
     *
     * <p>There are three {@link StateBinderFactory} implementations used by the {@link
     * CopyOnAccessInMemoryStateTable}.
     *
     * <ul>
     *   <li>The default {@link StateBinderFactory} is a {@link CopyOnBindBinderFactory}, allowing
     *       the table to copy any existing {@link State} values to this {@link StateTable} from the
     *       underlying table when accessed, at which point mutations will not be visible to the
     *       underlying table - effectively a "Copy by Value" binder.
     *   <li>During the execution of the {@link #commit()} method, this is a {@link
     *       ReadThroughBinderFactory}, which copies the references to the existing {@link State}
     *       objects to this {@link StateTable}.
     *   <li>After the execution of the {@link #commit()} method, this is an instance of {@link
     *       InMemoryStateBinderFactory}, which constructs new instances of state when a {@link
     *       StateTag} is bound.
     * </ul>
     */
    private StateBinderFactory binderFactory;

    /** The earliest watermark hold in this table. */
    private Optional<Instant> earliestWatermarkHold;

    public CopyOnAccessInMemoryStateTable(StateTable underlying) {
      this.underlying = Optional.ofNullable(underlying);
      binderFactory = new CopyOnBindBinderFactory(this.underlying);
      earliestWatermarkHold = Optional.empty();
    }

    /**
     * Copies all values in the underlying table to this table, then discards the underlying table.
     *
     * <p>If there is an underlying table, this replaces the existing {@link
     * CopyOnBindBinderFactory} with a {@link ReadThroughBinderFactory}, then reads all of the
     * values in the existing table, binding the state values to this table. The old StateTable
     * should be discarded after the call to {@link #commit()}.
     *
     * <p>After copying all of the existing values, replace the binder factory with an instance of
     * {@link InMemoryStateBinderFactory} to construct new values, since all existing values are
     * bound in this {@link StateTable table} and this table represents the canonical state.
     */
    private void commit() {
      Instant earliestHold = getEarliestWatermarkHold();
      if (underlying.isPresent()) {
        ReadThroughBinderFactory readThroughBinder =
            new ReadThroughBinderFactory<>(underlying.get());
        binderFactory = readThroughBinder;
        Instant earliestUnderlyingHold = readThroughBinder.readThroughAndGetEarliestHold(this);
        if (earliestUnderlyingHold.isBefore(earliestHold)) {
          earliestHold = earliestUnderlyingHold;
        }
      }
      earliestWatermarkHold = Optional.of(earliestHold);
      clearEmpty();
      binderFactory = new InMemoryStateBinderFactory();
      underlying = Optional.empty();
    }

    /**
     * Get the earliest watermark hold in this table. Ignores the contents of any underlying table.
     */
    private Instant getEarliestWatermarkHold() {
      Instant earliest = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (State existingState : this.values()) {
        if (existingState instanceof WatermarkHoldState) {
          Instant hold = ((WatermarkHoldState) existingState).read();
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
     * checked to ensure that it contains state and removed if not (otherwise we may never use the
     * table again).
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
    protected StateBinder binderForNamespace(final StateNamespace namespace, StateContext<?> c) {
      return binderFactory.forNamespace(namespace, c);
    }

    private interface StateBinderFactory {
      StateBinder forNamespace(StateNamespace namespace, StateContext<?> c);
    }

    /**
     * {@link StateBinderFactory} that creates a copy of any existing state when the state is bound.
     */
    private static class CopyOnBindBinderFactory implements StateBinderFactory {
      private final Optional<StateTable> underlying;

      public CopyOnBindBinderFactory(Optional<StateTable> underlying) {
        this.underlying = underlying;
      }

      private boolean containedInUnderlying(StateNamespace namespace, StateTag<?> tag) {
        return underlying.isPresent()
            && underlying.get().isNamespaceInUse(namespace)
            && underlying.get().getTagsInUse(namespace).containsKey(tag);
      }

      @Override
      public StateBinder forNamespace(final StateNamespace namespace, final StateContext<?> c) {
        return new StateBinder() {
          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends WatermarkHoldState> existingState =
                  (InMemoryState<? extends WatermarkHoldState>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryWatermarkHold<>(timestampCombiner);
            }
          }

          @Override
          public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends ValueState<T>> existingState =
                  (InMemoryState<? extends ValueState<T>>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryValue<>(coder);
            }
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineFn<InputT, AccumT, OutputT> combineFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends CombiningState<InputT, AccumT, OutputT>> existingState =
                  (InMemoryState<? extends CombiningState<InputT, AccumT, OutputT>>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryCombiningState<>(combineFn, accumCoder);
            }
          }

          @Override
          public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends BagState<T>> existingState =
                  (InMemoryState<? extends BagState<T>>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryBag<>(elemCoder);
            }
          }

          @Override
          public <T> SetState<T> bindSet(StateTag<SetState<T>> address, Coder<T> elemCoder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends SetState<T>> existingState =
                  (InMemoryState<? extends SetState<T>>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemorySet<>(elemCoder);
            }
          }

          @Override
          public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
              StateTag<MapState<KeyT, ValueT>> address,
              Coder<KeyT> mapKeyCoder,
              Coder<ValueT> mapValueCoder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends MapState<KeyT, ValueT>> existingState =
                  (InMemoryState<? extends MapState<KeyT, ValueT>>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryMap<>(mapKeyCoder, mapValueCoder);
            }
          }

          @Override
          public <T> OrderedListState<T> bindOrderedList(
              StateTag<OrderedListState<T>> address, Coder<T> elemCoder) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends OrderedListState<T>> existingState =
                  (InMemoryState<? extends OrderedListState<T>>)
                      underlying.get().get(namespace, address, c);
              return existingState.copy();
            } else {
              return new InMemoryOrderedList<>(elemCoder);
            }
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
          }
        };
      }
    }

    /**
     * {@link StateBinderFactory} that reads directly from the underlying table. Used during calls
     * to {@link CopyOnAccessInMemoryStateTable#commit()} to read all values from the underlying
     * table.
     */
    private static class ReadThroughBinderFactory<K> implements StateBinderFactory {
      private final StateTable underlying;

      public ReadThroughBinderFactory(StateTable underlying) {
        this.underlying = underlying;
      }

      public Instant readThroughAndGetEarliestHold(StateTable readTo) {
        Instant earliestHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
        for (StateNamespace namespace : underlying.getNamespacesInUse()) {
          for (Map.Entry<StateTag, State> existingState :
              underlying.getTagsInUse(namespace).entrySet()) {
            if (!((InMemoryState<?>) existingState.getValue()).isCleared()) {
              // Only read through non-cleared values to ensure that completed windows are
              // eventually discarded, and remember the earliest watermark hold from among those
              // values.
              State state =
                  readTo.get(namespace, existingState.getKey(), StateContexts.nullContext());
              if (state instanceof WatermarkHoldState) {
                Instant hold = ((WatermarkHoldState) state).read();
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
      public StateBinder forNamespace(final StateNamespace namespace, final StateContext<?> c) {
        return new StateBinder() {
          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineFn<InputT, AccumT, OutputT> combineFn) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <T> SetState<T> bindSet(StateTag<SetState<T>> address, Coder<T> elemCoder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
              StateTag<MapState<KeyT, ValueT>> address,
              Coder<KeyT> mapKeyCoder,
              Coder<ValueT> mapValueCoder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <T> OrderedListState<T> bindOrderedList(
              StateTag<OrderedListState<T>> address, Coder<T> elemCoder) {
            return underlying.get(namespace, address, c);
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
          }
        };
      }
    }

    private static class InMemoryStateBinderFactory implements StateBinderFactory {

      public InMemoryStateBinderFactory() {}

      @Override
      public StateBinder forNamespace(StateNamespace namespace, StateContext<?> c) {
        return new InMemoryStateBinder(c);
      }
    }
  }
}
