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

import java.io.Closeable;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;

final class CachingStateTable extends StateTable {
  private final String stateFamily;
  private final WindmillStateReader reader;
  private final WindmillStateCache.ForKeyAndFamily cache;
  private final boolean isSystemTable;
  private final Supplier<Closeable> scopedReadStateSupplier;
  private final @Nullable StateTable derivedStateTable;
  private final boolean isNewKey;
  private final boolean mapStateViaMultimapState;

  private CachingStateTable(Builder builder) {
    this.stateFamily = builder.stateFamily;
    this.reader = builder.reader;
    this.cache = builder.cache;
    this.isSystemTable = builder.isSystemTable;
    this.isNewKey = builder.isNewKey;
    this.scopedReadStateSupplier = builder.scopedReadStateSupplier;
    this.derivedStateTable = builder.derivedStateTable;
    this.mapStateViaMultimapState = builder.mapStateViaMultimapState;

    if (this.isSystemTable) {
      Preconditions.checkState(derivedStateTable == null);
    } else {
      Preconditions.checkNotNull(this.derivedStateTable);
    }
  }

  static CachingStateTable.Builder builder(
      String stateFamily,
      WindmillStateReader reader,
      WindmillStateCache.ForKeyAndFamily cache,
      boolean isNewKey,
      Supplier<Closeable> scopedReadStateSupplier) {
    return new CachingStateTable.Builder(
        stateFamily, reader, cache, scopedReadStateSupplier, isNewKey);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected StateTag.StateBinder binderForNamespace(StateNamespace namespace, StateContext<?> c) {
    // Look up state objects in the cache or create new ones if not found.  The state will
    // be added to the cache in persist().
    return new StateTag.StateBinder() {
      @Override
      public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
        StateTag<BagState<T>> resolvedAddress =
            isSystemTable ? StateTags.makeSystemTagInternal(address) : address;

        WindmillBag<T> result =
            cache
                .get(namespace, resolvedAddress)
                .map(bagState -> (WindmillBag<T>) bagState)
                .orElseGet(
                    () ->
                        new WindmillBag<>(
                            namespace, resolvedAddress, stateFamily, elemCoder, isNewKey));

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
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
          result =
              cache
                  .get(namespace, spec)
                  .map(mapState -> (AbstractWindmillMap<KeyT, ValueT>) mapState)
                  .orElseGet(
                      () ->
                          new WindmillMap<>(
                              namespace, spec, stateFamily, keyCoder, valueCoder, isNewKey));
        }
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <KeyT, ValueT> WindmillMultimap<KeyT, ValueT> bindMultimap(
          StateTag<MultimapState<KeyT, ValueT>> spec,
          Coder<KeyT> keyCoder,
          Coder<ValueT> valueCoder) {
        WindmillMultimap<KeyT, ValueT> result =
            cache
                .get(namespace, spec)
                .map(multimapState -> (WindmillMultimap<KeyT, ValueT>) multimapState)
                .orElseGet(
                    () ->
                        new WindmillMultimap<>(
                            namespace, spec, stateFamily, keyCoder, valueCoder, isNewKey));
        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <T> OrderedListState<T> bindOrderedList(
          StateTag<OrderedListState<T>> spec, Coder<T> elemCoder) {
        StateTag<OrderedListState<T>> specOrInternalTag = addressOrInternalTag(spec);

        WindmillOrderedList<T> result =
            cache
                .get(namespace, specOrInternalTag)
                .map(orderedList -> (WindmillOrderedList<T>) orderedList)
                .orElseGet(
                    () ->
                        new WindmillOrderedList<>(
                            Optional.ofNullable(derivedStateTable).orElse(CachingStateTable.this),
                            namespace,
                            specOrInternalTag,
                            stateFamily,
                            elemCoder,
                            isNewKey));

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public WatermarkHoldState bindWatermark(
          StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
        StateTag<WatermarkHoldState> addressOrInternalTag = addressOrInternalTag(address);

        WindmillWatermarkHold result =
            cache
                .get(namespace, addressOrInternalTag)
                .map(watermarkHold -> (WindmillWatermarkHold) watermarkHold)
                .orElseGet(
                    () ->
                        new WindmillWatermarkHold(
                            namespace, address, stateFamily, timestampCombiner, isNewKey));

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
          StateTag<CombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
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
                isNewKey);

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      @Override
      public <InputT, AccumT, OutputT>
          CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
              StateTag<CombiningState<InputT, AccumT, OutputT>> address,
              Coder<AccumT> accumCoder,
              CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        return bindCombiningValue(
            addressOrInternalTag(address), accumCoder, CombineFnUtil.bindContext(combineFn, c));
      }

      @Override
      public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
        StateTag<ValueState<T>> addressOrInternalTag = addressOrInternalTag(address);

        WindmillValue<T> result =
            cache
                .get(namespace, addressOrInternalTag)
                .map(value -> (WindmillValue<T>) value)
                .orElseGet(
                    () ->
                        new WindmillValue<>(
                            namespace, addressOrInternalTag, stateFamily, coder, isNewKey));

        result.initializeForWorkItem(reader, scopedReadStateSupplier);
        return result;
      }

      private <T extends State> StateTag<T> addressOrInternalTag(StateTag<T> address) {
        return isSystemTable ? StateTags.makeSystemTagInternal(address) : address;
      }
    };
  }

  static class Builder {
    private final String stateFamily;
    private final WindmillStateReader reader;
    private final WindmillStateCache.ForKeyAndFamily cache;
    private final Supplier<Closeable> scopedReadStateSupplier;
    private final boolean isNewKey;
    private boolean isSystemTable;
    private @Nullable StateTable derivedStateTable;
    private boolean mapStateViaMultimapState = false;

    private Builder(
        String stateFamily,
        WindmillStateReader reader,
        WindmillStateCache.ForKeyAndFamily cache,
        Supplier<Closeable> scopedReadStateSupplier,
        boolean isNewKey) {
      this.stateFamily = stateFamily;
      this.reader = reader;
      this.cache = cache;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
      this.isNewKey = isNewKey;
      this.isSystemTable = true;
      this.derivedStateTable = null;
    }

    Builder withDerivedState(StateTable derivedStateTable) {
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
