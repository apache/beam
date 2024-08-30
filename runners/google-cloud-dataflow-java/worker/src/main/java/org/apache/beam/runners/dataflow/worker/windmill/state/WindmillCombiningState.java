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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

@NotThreadSafe
class WindmillCombiningState<InputT, AccumT, OutputT> extends WindmillState
    implements CombiningState<InputT, AccumT, OutputT> {

  private final WindmillBag<AccumT> bag;
  private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

  /* We use a separate, in-memory AccumT rather than relying on the WindmillWatermarkBag's
   * localAdditions, because we want to combine multiple InputT's to a single AccumT
   * before adding it.
   */
  private AccumT localAdditionsAccumulator;
  private boolean hasLocalAdditions;

  WindmillCombiningState(
      StateNamespace namespace,
      StateTag<CombiningState<InputT, AccumT, OutputT>> address,
      String stateFamily,
      Coder<AccumT> accumCoder,
      Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
      WindmillStateCache.ForKeyAndFamily cache,
      boolean isNewKey) {
    StateTag<BagState<AccumT>> internalBagAddress = StateTags.convertToBagTagInternal(address);
    this.bag =
        cache
            .get(namespace, internalBagAddress)
            .map(state -> (WindmillBag<AccumT>) state)
            .orElseGet(
                () ->
                    new WindmillBag<>(
                        namespace, internalBagAddress, stateFamily, accumCoder, isNewKey));

    this.combineFn = combineFn;
    this.localAdditionsAccumulator = combineFn.createAccumulator();
    this.hasLocalAdditions = false;
  }

  @Override
  void initializeForWorkItem(
      WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
    super.initializeForWorkItem(reader, scopedReadStateSupplier);
    this.bag.initializeForWorkItem(reader, scopedReadStateSupplier);
  }

  @Override
  void cleanupAfterWorkItem() {
    super.cleanupAfterWorkItem();
    bag.cleanupAfterWorkItem();
  }

  @Override
  public WindmillCombiningState<InputT, AccumT, OutputT> readLater() {
    bag.readLater();
    return this;
  }

  @Override
  @SuppressWarnings("nullness")
  public OutputT read() {
    return combineFn.extractOutput(getAccum());
  }

  @Override
  public void add(InputT input) {
    hasLocalAdditions = true;
    localAdditionsAccumulator = combineFn.addInput(localAdditionsAccumulator, input);
  }

  @Override
  public void clear() {
    bag.clear();
    localAdditionsAccumulator = combineFn.createAccumulator();
    hasLocalAdditions = false;
  }

  @Override
  public Future<Windmill.WorkItemCommitRequest> persist(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    if (hasLocalAdditions) {
      if (WindmillStateInternals.COMPACT_NOW.get().get() || bag.valuesAreCached()) {
        // Implicitly clears the bag and combines local and persisted accumulators.
        localAdditionsAccumulator = getAccum();
      }
      bag.add(combineFn.compact(localAdditionsAccumulator));
      localAdditionsAccumulator = combineFn.createAccumulator();
      hasLocalAdditions = false;
    }

    return bag.persist(cache);
  }

  @Override
  public AccumT getAccum() {
    Iterable<AccumT> accumulators =
        Iterables.concat(bag.read(), Collections.singleton(localAdditionsAccumulator));

    // Compact things
    AccumT merged = combineFn.mergeAccumulators(accumulators);
    bag.clear();
    localAdditionsAccumulator = merged;
    hasLocalAdditions = true;
    return merged;
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    final ReadableState<Boolean> bagIsEmpty = bag.isEmpty();
    return new ReadableState<Boolean>() {
      @Override
      public ReadableState<Boolean> readLater() {
        bagIsEmpty.readLater();
        return this;
      }

      @Override
      public Boolean read() {
        return !hasLocalAdditions && bagIsEmpty.read();
      }
    };
  }

  @Override
  public void addAccum(AccumT accumulator) {
    hasLocalAdditions = true;
    localAdditionsAccumulator =
        combineFn.mergeAccumulators(Arrays.asList(localAdditionsAccumulator, accumulator));
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    return combineFn.mergeAccumulators(accumulators);
  }
}
