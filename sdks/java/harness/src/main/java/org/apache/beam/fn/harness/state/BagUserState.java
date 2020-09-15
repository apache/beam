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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * An implementation of a bag user state that utilizes the Beam Fn State API to fetch, clear and
 * persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache memory
 * pressure and its need to flush.
 *
 * <p>TODO: Support block level caching and prefetch.
 */
public class BagUserState<T> {
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest request;
  private final Coder<T> valueCoder;
  private Iterable<T> oldValues;
  private ArrayList<T> newValues;
  private boolean isClosed;

  public BagUserState(
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      String ptransformId,
      String stateId,
      ByteString encodedWindow,
      ByteString encodedKey,
      Coder<T> valueCoder) {
    this.beamFnStateClient = beamFnStateClient;
    this.valueCoder = valueCoder;

    StateRequest.Builder requestBuilder = StateRequest.newBuilder();
    requestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getBagUserStateBuilder()
        .setTransformId(ptransformId)
        .setUserStateId(stateId)
        .setWindow(encodedWindow)
        .setKey(encodedKey);
    request = requestBuilder.build();

    this.oldValues =
        new LazyCachingIteratorToIterable<>(
            new DataStreams.DataStreamDecoder(
                valueCoder,
                DataStreams.inbound(
                    StateFetchingIterators.readAllStartingFrom(beamFnStateClient, request))));
    this.newValues = new ArrayList<>();
  }

  public Iterable<T> get() {
    checkState(
        !isClosed,
        "Bag user state is no longer usable because it is closed for %s",
        request.getStateKey());
    if (oldValues == null) {
      // If we were cleared we should disregard old values.
      return Iterables.limit(Collections.unmodifiableList(newValues), newValues.size());
    } else if (newValues.isEmpty()) {
      // If we have no new values then just return the old values.
      return oldValues;
    }
    return Iterables.concat(
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
    oldValues = null;
    newValues = new ArrayList<>();
  }

  public void asyncClose() throws Exception {
    checkState(
        !isClosed,
        "Bag user state is no longer usable because it is closed for %s",
        request.getStateKey());
    if (oldValues == null) {
      beamFnStateClient.handle(
          request.toBuilder().setClear(StateClearRequest.getDefaultInstance()),
          new CompletableFuture<>());
    }
    if (!newValues.isEmpty()) {
      ByteString.Output out = ByteString.newOutput();
      for (T newValue : newValues) {
        // TODO: Replace with chunking output stream
        valueCoder.encode(newValue, out);
      }
      beamFnStateClient.handle(
          request
              .toBuilder()
              .setAppend(StateAppendRequest.newBuilder().setData(out.toByteString())),
          new CompletableFuture<>());
    }
    isClosed = true;
  }
}
