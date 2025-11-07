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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import com.google.auto.value.AutoValue;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache.ForKeyAndFamily;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;

final class CachingStateTable {

  private final HashMap<StateTableKey, State> stateTable;
  private final String stateFamily;
  private final WindmillStateReader reader;
  private final WindmillStateCache.ForKeyAndFamily cache;
  private final boolean isSystemTable;
  private final Supplier<Closeable> scopedReadStateSupplier;
  private final @Nullable CachingStateTable derivedStateTable;
  private final boolean isNewKey;
  private final boolean mapStateViaMultimapState;
  private final WindmillStateTagUtil windmillStateTagUtil;

  private CachingStateTable(Builder builder) {
    this.stateTable = new HashMap<>();
    this.stateFamily = builder.stateFamily;
    this.reader = builder.reader;
    this.cache = builder.cache;
    this.isSystemTable = builder.isSystemTable;
    this.isNewKey = builder.isNewKey;
    this.scopedReadStateSupplier = builder.scopedReadStateSupplier;
    this.derivedStateTable = builder.derivedStateTable;
    this.mapStateViaMultimapState = builder.mapStateViaMultimapState;
    this.windmillStateTagUtil = builder.windmillStateTagUtil;
    if (this.isSystemTable) {
      Preconditions.checkState(derivedStateTable == null);
    } else {
      Preconditions.checkNotNull(this.derivedStateTable);
    }
  }

  static Builder builder(
      String stateFamily,
      WindmillStateReader reader,
      ForKeyAndFamily cache,
      boolean isNewKey,
      Supplier<Closeable> scopedReadStateSupplier,
      WindmillStateTagUtil windmillStateTagUtil) {
    return new Builder(
        stateFamily, reader, cache, scopedReadStateSupplier, isNewKey, windmillStateTagUtil);
  }

  /**
   * Gets the {@link State} in the specified {@link StateNamespace} with the specified {@link
   * StateTag}, binding it using the {@link #binderForNamespace} if it is not already present in
   * this {@link CachingStateTable}.
   */
  public <StateT extends State> StateT get(
      StateNamespace namespace, StateTag<StateT> tag, StateContext<?> c) {

    StateTableKey stateTableKey = StateTableKey.create(namespace, tag);
    @org.checkerframework.checker.nullness.qual.Nullable
    State storage = stateTable.get(stateTableKey);
    if (storage != null) {
      @SuppressWarnings("unchecked")
      StateT typedStorage = (StateT) storage;
      return typedStorage;
    }

    StateT typedStorage = tag.bind(binderForNamespace(namespace, c));
    stateTable.put(stateTableKey, typedStorage);
    return typedStorage;
  }

  public void clear() {
    stateTable.clear();
  }

  public Iterable<State> values() {
    return stateTable.values();
  }

  @SuppressWarnings("deprecation")
  private StateTag.StateBinder binderForNamespace(StateNamespace namespace, StateContext<?> c) {
    // Look up state objects in the cache or create new ones if not found.  The state will
    // be added to the cache in persist().
    return new StateTag.StateBinder() {
      @Override
      public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
        StateTag<BagState<T>> resolvedAddress =
            isSystemTable ? StateTags.makeSystemTagInternal(address) : address;
        InternedByteString encodedKey = windmillStateTagUtil.encodeKey(namespace, resolvedAddress);

        @Nullable WindmillBag<T> bag = (WindmillBag<T>) cache.get(namespace, encodedKey);
        if (bag == null) {
          bag = new WindmillBag<>(namespace, encodedKey, stateFamily, elemCoder, isNewKey);
        }
        bag.initializeForWorkItem(reader, scopedReadStateSupplier);
        return bag;
      }

      @Override
      public <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder) {
        StateTag<MapState<T, Boolean>> internalMapAddress = StateTags.convertToMapTagInternal(spec);
        WindmillSet<T> result =
            new WindmillSet<>(bindMap(internalMapAddress, elemCoder, BooleanCoder.of()));
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <KeyT, ValueT> AbstractWindmillMap<KeyT, ValueT> bindMap(
          StateTag<MapState<KeyT, ValueT>> spec, Coder<KeyT> keyCoder, Coder<ValueT> valueCoder) {
        AbstractWindmillMap<KeyT, ValueT> result;
        if (mapStateViaMultimapState) {
          StateTag<MultimapState<KeyT, ValueT>> internalMultimapAddress =
              StateTags.convertToMultiMapTagInternal(spec);
          result =
              new WindmillMapViaMultimap<>(
                  bindMultimap(internalMultimapAddress, keyCoder, valueCoder));
        } else {
          InternedByteString encodedKey = windmillStateTagUtil.encodeKey(namespace, spec);
          result = (AbstractWindmillMap<KeyT, ValueT>) cache.get(namespace, encodedKey);
          if (result == null) {
            result =
                new WindmillMap<>(
                    namespace, encodedKey, stateFamily, keyCoder, valueCoder, isNewKey);
          }
        }
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <KeyT, ValueT> WindmillMultimap<KeyT, ValueT> bindMultimap(
          StateTag<MultimapState<KeyT, ValueT>> spec,
          Coder<KeyT> keyCoder,
          Coder<ValueT> valueCoder) {
        InternedByteString encodedKey = windmillStateTagUtil.encodeKey(namespace, spec);
        WindmillMultimap<KeyT, ValueT> result =
            (WindmillMultimap<KeyT, ValueT>) cache.get(namespace, encodedKey);
        if (result == null) {
          result =
              new WindmillMultimap<>(
                  namespace, encodedKey, stateFamily, keyCoder, valueCoder, isNewKey);
        }
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <T> OrderedListState<T> bindOrderedList(
          StateTag<OrderedListState<T>> spec, Coder<T> elemCoder) {
        StateTag<OrderedListState<T>> specOrInternalTag = addressOrInternalTag(spec);
        InternedByteString encodedKey =
            windmillStateTagUtil.encodeKey(namespace, specOrInternalTag);

        WindmillOrderedList<T> result = (WindmillOrderedList<T>) cache.get(namespace, encodedKey);
        if (result == null) {
          result =
              new WindmillOrderedList<>(
                  Optional.ofNullable(derivedStateTable).orElse(CachingStateTable.this),
                  namespace,
                  encodedKey,
                  specOrInternalTag,
                  stateFamily,
                  elemCoder,
                  isNewKey);
        }

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public WatermarkHoldState bindWatermark(
          StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
        StateTag<WatermarkHoldState> addressOrInternalTag = addressOrInternalTag(address);
        InternedByteString encodedKey =
            windmillStateTagUtil.encodeKey(namespace, addressOrInternalTag);

        WindmillWatermarkHold result = (WindmillWatermarkHold) cache.get(namespace, encodedKey);
        if (result == null) {
          result =
              new WindmillWatermarkHold(
                  namespace, encodedKey, stateFamily, timestampCombiner, isNewKey);
        }
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
          StateTag<CombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          CombineFn<InputT, AccumT, OutputT> combineFn) {
        StateTag<CombiningState<InputT, AccumT, OutputT>> addressOrInternalTag =
            addressOrInternalTag(address);

        WindmillCombiningState<InputT, AccumT, OutputT> result =
            new WindmillCombiningState<>(
                namespace,
                addressOrInternalTag,
                stateFamily,
                accumCoder,
                combineFn,
                cache,
                isNewKey,
                windmillStateTagUtil);

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <InputT, AccumT, OutputT>
          CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
              StateTag<CombiningState<InputT, AccumT, OutputT>> address,
              Coder<AccumT> accumCoder,
              CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        return bindCombiningValue(
            addressOrInternalTag(address), accumCoder, CombineFnUtil.bindContext(combineFn, c));
      }

      @Override
      public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
        StateTag<ValueState<T>> addressOrInternalTag = addressOrInternalTag(address);
        InternedByteString encodedKey =
            windmillStateTagUtil.encodeKey(namespace, addressOrInternalTag);

        WindmillValue<T> result = (WindmillValue<T>) cache.get(namespace, encodedKey);
        if (result == null) {
          result = new WindmillValue<>(namespace, encodedKey, stateFamily, coder, isNewKey);
        }
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      private <T extends State> StateTag<T> addressOrInternalTag(StateTag<T> address) {
        return isSystemTable ? StateTags.makeSystemTagInternal(address) : address;
      }
    };
  }

  @AutoValue
  abstract static class StateTableKey {

    public abstract StateNamespace getStateNameSpace();

    public abstract String getId();

    public static StateTableKey create(StateNamespace namespace, StateTag<?> stateTag) {
      return new AutoValue_CachingStateTable_StateTableKey(namespace, stateTag.getId());
    }
  }

  static class Builder {

    private final String stateFamily;
    private final WindmillStateReader reader;
    private final WindmillStateCache.ForKeyAndFamily cache;
    private final Supplier<Closeable> scopedReadStateSupplier;
    private final boolean isNewKey;
    private final WindmillStateTagUtil windmillStateTagUtil;
    private boolean isSystemTable;
    private @Nullable CachingStateTable derivedStateTable;
    private boolean mapStateViaMultimapState = false;

    private Builder(
        String stateFamily,
        WindmillStateReader reader,
        ForKeyAndFamily cache,
        Supplier<Closeable> scopedReadStateSupplier,
        boolean isNewKey,
        WindmillStateTagUtil windmillStateTagUtil) {
      this.stateFamily = stateFamily;
      this.reader = reader;
      this.cache = cache;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
      this.isNewKey = isNewKey;
      this.isSystemTable = true;
      this.derivedStateTable = null;
      this.windmillStateTagUtil = windmillStateTagUtil;
    }

    Builder withDerivedState(CachingStateTable derivedStateTable) {
      this.isSystemTable = false;
      this.derivedStateTable = derivedStateTable;
      return this;
    }

    Builder withMapStateViaMultimapState() {
      this.mapStateViaMultimapState = true;
      return this;
    }

    CachingStateTable build() {
      return new CachingStateTable(this);
    }
  }
}
