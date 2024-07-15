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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Implementation of {@link StateInternals} using Windmill to manage the underlying data. */
@SuppressWarnings("nullness" // TODO(https://github.com/apache/beam/issues/20497)
)
public class WindmillStateInternals<K> implements StateInternals {

  @VisibleForTesting
  static final ThreadLocal<Supplier<Boolean>> COMPACT_NOW =
      ThreadLocal.withInitial(ShouldCompactNowFn::new);
  /**
   * The key will be null when not in a keyed context, from the users perspective. There is still a
   * "key" for the Windmill computation, but it cannot be meaningfully deserialized.
   */
  private final @Nullable K key;

  private final WindmillStateCache.ForKeyAndFamily cache;
  private final StateTable workItemState;
  private final StateTable workItemDerivedState;
  private final Supplier<Closeable> scopedReadStateSupplier;

  public WindmillStateInternals(
      @Nullable K key,
      String stateFamily,
      WindmillStateReader reader,
      boolean isNewKey,
      WindmillStateCache.ForKeyAndFamily cache,
      Supplier<Closeable> scopedReadStateSupplier) {
    this.key = key;
    this.cache = cache;
    this.scopedReadStateSupplier = scopedReadStateSupplier;
    CachingStateTable.Builder builder =
        CachingStateTable.builder(stateFamily, reader, cache, isNewKey, scopedReadStateSupplier);
    if (cache.supportMapStateViaMultimapState()) {
      builder = builder.withMapStateViaMultimapState();
    }
    this.workItemDerivedState = builder.build();
    this.workItemState = builder.withDerivedState(workItemDerivedState).build();
  }

  @Override
  public @Nullable K getKey() {
    return key;
  }

  private void persist(List<Future<WorkItemCommitRequest>> commitsToMerge, StateTable stateTable) {
    for (State location : stateTable.values()) {
      if (!(location instanceof WindmillState)) {
        throw new IllegalStateException(
            String.format(
                "%s wasn't created by %s -- unable to persist it",
                location.getClass().getSimpleName(), getClass().getSimpleName()));
      }

      try {
        commitsToMerge.add(((WindmillState) location).persist(cache));
      } catch (IOException e) {
        throw new RuntimeException("Unable to persist state", e);
      }
    }

    // All cached State objects now have known values.
    // Clear any references to the underlying reader to prevent space leaks.
    // The next work unit to use these cached State objects will reset the
    // reader to a current reader in case those values are modified.
    for (State location : stateTable.values()) {
      ((WindmillState) location).cleanupAfterWorkItem();
    }

    // Clear out the map of already retrieved state instances.
    stateTable.clear();
  }

  public void persist(final Windmill.WorkItemCommitRequest.Builder commitBuilder) {
    List<Future<WorkItemCommitRequest>> commitsToMerge = new ArrayList<>();

    // Call persist on each first, which may schedule some futures for reading.
    persist(commitsToMerge, workItemState);
    persist(commitsToMerge, workItemDerivedState);

    try (Closeable ignored = scopedReadStateSupplier.get()) {
      for (Future<WorkItemCommitRequest> commitFuture : commitsToMerge) {
        commitBuilder.mergeFrom(commitFuture.get());
      }
    } catch (ExecutionException | InterruptedException | IOException exc) {
      if (exc instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed to retrieve Windmill state during persist()", exc);
    }

    cache.persist();
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
    return workItemState.get(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return workItemState.get(namespace, address, c);
  }

  private static class ShouldCompactNowFn implements Supplier<Boolean> {
    /* The rate at which, on average, this will return true. */
    private static final double RATE = 0.002;
    private final Random random;
    private long counter;

    private ShouldCompactNowFn() {
      this.random = new Random();
      this.counter = nextSample(random);
    }

    private static long nextSample(Random random) {
      // Use geometric distribution to find next true value.
      // This lets us avoid invoking random.nextDouble() on every call.
      return (long) Math.floor(Math.log(random.nextDouble()) / Math.log(1 - RATE));
    }

    @Override
    public Boolean get() {
      counter--;
      if (counter < 0) {
        counter = nextSample(random);
        return true;
      } else {
        return false;
      }
    }
  }
}
