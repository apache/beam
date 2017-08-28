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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.stream.DataStreams;
import org.apache.beam.fn.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.fn.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.fn.v1.BeamFnApi.StateRequest.Builder;
import org.apache.beam.sdk.coders.Coder;

/**
 * An implementation of a bag user state that utilizes the Beam Fn State API to fetch, clear
 * and persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache
 * memory pressure and its need to flush.
 *
 * <p>TODO: Support block level caching and prefetch.
 */
public class BagUserState<T> {
  private final BeamFnStateClient beamFnStateClient;
  private final String stateId;
  private final Coder<T> coder;
  private final Supplier<Builder> partialRequestSupplier;
  private Iterable<T> oldValues;
  private ArrayList<T> newValues;
  private List<T> unmodifiableNewValues;
  private boolean isClosed;

  public BagUserState(
      BeamFnStateClient beamFnStateClient,
      String stateId,
      Coder<T> coder,
      Supplier<Builder> partialRequestSupplier) {
    this.beamFnStateClient = beamFnStateClient;
    this.stateId = stateId;
    this.coder = coder;
    this.partialRequestSupplier = partialRequestSupplier;
    this.oldValues = new LazyCachingIteratorToIterable<>(
        new DataStreams.DataStreamDecoder(coder,
            DataStreams.inbound(
                StateFetchingIterators.usingPartialRequestWithStateKey(
                    beamFnStateClient,
                    partialRequestSupplier))));
    this.newValues = new ArrayList<>();
    this.unmodifiableNewValues = Collections.unmodifiableList(newValues);
  }

  public Iterable<T> get() {
    checkState(!isClosed,
        "Bag user state is no longer usable because it is closed for %s", stateId);
    // If we were cleared we should disregard old values.
    if (oldValues == null) {
      return unmodifiableNewValues;
    }
    return Iterables.concat(oldValues, unmodifiableNewValues);
  }

  public void append(T t) {
    checkState(!isClosed,
        "Bag user state is no longer usable because it is closed for %s", stateId);
    newValues.add(t);
  }

  public void clear() {
    checkState(!isClosed,
        "Bag user state is no longer usable because it is closed for %s", stateId);
    oldValues = null;
    newValues.clear();
  }

  public void asyncClose() throws Exception {
    checkState(!isClosed,
        "Bag user state is no longer usable because it is closed for %s", stateId);
    if (oldValues == null) {
      beamFnStateClient.handle(
          partialRequestSupplier.get()
              .setClear(StateClearRequest.getDefaultInstance()),
          new CompletableFuture<>());
    }
    if (!newValues.isEmpty()) {
      ByteString.Output out = ByteString.newOutput();
      for (T newValue : newValues) {
        // TODO: Replace with chunking output stream
        coder.encode(newValue, out);
      }
      beamFnStateClient.handle(
          partialRequestSupplier.get()
              .setAppend(StateAppendRequest.newBuilder().setData(out.toByteString())),
          new CompletableFuture<>());
    }
    isClosed = true;
  }
}
