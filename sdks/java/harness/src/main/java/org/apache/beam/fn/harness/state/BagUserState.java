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
package org.apache.beam.fn.harness.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterables;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * An implementation of a bag user state that utilizes the Beam Fn State API to fetch, clear and
 * persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache memory
 * pressure and its need to flush.
 */
public class BagUserState<T> {
  private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest request;
  private final Coder<T> valueCoder;
  private final CachingStateIterable<T> oldValues;
  private List<T> newValues;
  private boolean isCleared;
  private boolean isClosed;

  static final int BAG_APPEND_BATCHING_LIMIT = 10 * 1024 * 1024;

  /** The cache must be namespaced for this state object accordingly. */
  public BagUserState(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      Coder<T> valueCoder) {
    checkArgument(
        stateKey.hasBagUserState(), "Expected BagUserState StateKey but received %s.", stateKey);
    this.cache = cache;
    this.beamFnStateClient = beamFnStateClient;
    this.valueCoder = valueCoder;
    this.request =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();

    this.oldValues =
        StateFetchingIterators.readAllAndDecodeStartingFrom(
            this.cache, beamFnStateClient, request, valueCoder);
    this.newValues = new ArrayList<>();
  }

  public PrefetchableIterable<T> get() {
    checkState(
        !isClosed,
        "Bag user state is no longer usable because it is closed for %s",
        request.getStateKey());
    if (isCleared) {
      // If we were cleared we should disregard old values.
      return PrefetchableIterables.limit(Collections.unmodifiableList(newValues), newValues.size());
    } else if (newValues.isEmpty()) {
      // If we have no new values then just return the old values.
      return oldValues;
    }
    return PrefetchableIterables.concat(
        oldValues, Iterables.limit(Collections.unmodifiableList(newValues), newValues.size()));
  }

  public void append(T t) {
    checkState(
        !isClosed,
        "Bag user state is no longer usable because it is closed for %s",
        request.getStateKey());
    newValues.add(t);
  }

  public void clear() {
    checkState(
        !isClosed,
        "Bag user state is no longer usable because it is closed for %s",
        request.getStateKey());
    isCleared = true;
    newValues = new ArrayList<>();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void asyncClose() throws Exception {
    checkState(
        !isClosed,
        "Bag user state is no longer usable because it is closed for %s",
        request.getStateKey());
    isClosed = true;
    if (!isCleared && newValues.isEmpty()) {
      return;
    }
    if (isCleared) {
      beamFnStateClient.handle(
          request.toBuilder().setClear(StateClearRequest.getDefaultInstance()));
    }
    if (!newValues.isEmpty()) {
      // Batch values up to a arbitrary limit to reduce overhead of write
      // requests. We treat this limit as strict to ensure that large elements
      // are not batched as they may otherwise exceed runner limits.
      ByteStringOutputStream out = new ByteStringOutputStream();
      for (T newValue : newValues) {
        int previousSize = out.size();
        valueCoder.encode(newValue, out);
        if (out.size() > BAG_APPEND_BATCHING_LIMIT && previousSize > 0) {
          // Respect the batching limit by outputting the previous batch of
          // elements.
          beamFnStateClient.handle(
              request
                  .toBuilder()
                  .setAppend(
                      StateAppendRequest.newBuilder()
                          .setData(out.consumePrefixToByteString(previousSize))));
        }
        if (out.size() > BAG_APPEND_BATCHING_LIMIT) {
          // The last element was over the batching limit by itself. To avoid
          // exceeding runner state limits due to large elements, we output
          // without additional batching.
          beamFnStateClient.handle(
              request
                  .toBuilder()
                  .setAppend(StateAppendRequest.newBuilder().setData(out.toByteStringAndReset())));
        }
      }
      if (out.size() > 0) {
        beamFnStateClient.handle(
            request
                .toBuilder()
                .setAppend(StateAppendRequest.newBuilder().setData(out.toByteStringAndReset())));
      }
    }

    // Modify the underlying cached state depending on the mutations performed
    if (isCleared) {
      // Note this takes ownership of newValues. This object is no longer used after it has been
      // closed.
      oldValues.clearAndAppend(newValues);
    } else {
      oldValues.append(newValues);
    }
  }
}
