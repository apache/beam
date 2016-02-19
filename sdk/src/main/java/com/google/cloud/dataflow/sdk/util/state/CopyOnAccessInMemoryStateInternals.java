/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.dataflow.sdk.util.state;

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.util.state.InMemoryStateInternals.InMemoryState;
import com.google.cloud.dataflow.sdk.util.state.StateTag.StateBinder;
import com.google.common.base.Optional;

import org.joda.time.Instant;

import java.util.Map;

import javax.annotation.Nullable;

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
   * <p>Additionally, stores the {@link WatermarkStateInternal} with the earliest time bound in the
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
    return table.get(namespace, address);
  }

  @Override
  public K getKey() {
    return key;
  }

  /**
   * A {@link StateTable} that, when a value is retrieved with
   * {@link StateTable#get(StateNamespace, StateTag)}, first attempts to obtain a copy of existing
   * {@link State} from an underlying {@link StateTable}.
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
      binderFactory = new InMemoryStateBinderFactory<>(key);
      underlying = Optional.absent();
    }

    /**
     * Get the earliest watermark hold in this table. Ignores the contents of any underlying table.
     */
    private Instant getEarliestWatermarkHold() {
      Instant earliest = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (State existingState : this.values()) {
        if (existingState instanceof WatermarkStateInternal) {
          Instant hold = ((WatermarkStateInternal<?>) existingState).get().read();
          if (hold != null && hold.isBefore(earliest)) {
            earliest = hold;
          }
        }
      }
      return earliest;
    }

    @Override
    protected StateBinder<K> binderForNamespace(final StateNamespace namespace) {
      return binderFactory.forNamespace(namespace);
    }

    private static interface StateBinderFactory<K> {
      StateBinder<K> forNamespace(StateNamespace namespace);
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
      public StateBinder<K> forNamespace(final StateNamespace namespace) {
        return new StateBinder<K>() {
          @Override
          public <W extends BoundedWindow> WatermarkStateInternal<W> bindWatermark(
              StateTag<? super K, WatermarkStateInternal<W>> address,
              OutputTimeFn<? super W> outputTimeFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends WatermarkStateInternal<W>> existingState =
                  (InMemoryStateInternals.InMemoryState<? extends WatermarkStateInternal<W>>)
                  underlying.get().get(namespace, address);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.WatermarkStateInternalImplementation<>(
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
                  underlying.get().get(namespace, address);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryValue<>();
            }
          }

          @Override
          public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT>
              bindCombiningValue(
                  StateTag<? super K, CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends CombiningValueStateInternal<InputT, AccumT, OutputT>>
                  existingState = (
                      InMemoryStateInternals
                          .InMemoryState<? extends CombiningValueStateInternal<InputT, AccumT,
                          OutputT>>) underlying.get().get(namespace, address);
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
                  underlying.get().get(namespace, address);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryBag<>();
            }
          }

          @Override
          public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT>
              bindKeyedCombiningValue(
                  StateTag<? super K, CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
            if (containedInUnderlying(namespace, address)) {
              @SuppressWarnings("unchecked")
              InMemoryState<? extends CombiningValueStateInternal<InputT, AccumT, OutputT>>
                  existingState = (
                      InMemoryStateInternals
                          .InMemoryState<? extends CombiningValueStateInternal<InputT, AccumT,
                          OutputT>>) underlying.get().get(namespace, address);
              return existingState.copy();
            } else {
              return new InMemoryStateInternals.InMemoryCombiningValue<>(key, combineFn);
            }
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
              State state = readTo.get(namespace, existingState.getKey());
              if (state instanceof WatermarkStateInternal) {
                Instant hold = ((WatermarkStateInternal<?>) state).get().read();
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
      public StateBinder<K> forNamespace(final StateNamespace namespace) {
        return new StateBinder<K>() {
          @Override
          public <W extends BoundedWindow> WatermarkStateInternal<W> bindWatermark(
              StateTag<? super K, WatermarkStateInternal<W>> address,
              OutputTimeFn<? super W> outputTimeFn) {
            return underlying.get(namespace, address);
          }

          @Override
          public <T> ValueState<T> bindValue(
              StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
            return underlying.get(namespace, address);
          }

          @Override
          public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT>
              bindCombiningValue(
                  StateTag<? super K, CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
            return underlying.get(namespace, address);
          }

          @Override
          public <T> BagState<T> bindBag(
              StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
            return underlying.get(namespace, address);
          }

          @Override
          public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT>
              bindKeyedCombiningValue(
                  StateTag<? super K, CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
            return underlying.get(namespace, address);
          }
        };
      }
    }

    private static class InMemoryStateBinderFactory<K> implements StateBinderFactory<K> {
      private final InMemoryStateInternals.InMemoryStateBinder<K> inMemoryStateBinder;

      public InMemoryStateBinderFactory(K key) {
        inMemoryStateBinder = new InMemoryStateInternals.InMemoryStateBinder<>(key);
      }

      @Override
      public StateBinder<K> forNamespace(StateNamespace namespace) {
        return inMemoryStateBinder;
      }
    }
  }

}
