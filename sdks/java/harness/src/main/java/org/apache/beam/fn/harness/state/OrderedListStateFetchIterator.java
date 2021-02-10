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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.beam.fn.harness.state.StateFetchingIterators.LazyBlockingStateFetchingIterator.State;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;

// TODO(boyuanz@): We should refactor StateFetchingIterators
public class OrderedListStateFetchIterator<T> implements Iterator<TimestampedValue<T>> {

  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest stateRequest;
  private final OrderedListStateGetRequest getRequestForFirstChunk;
  private final Coder<T> valueCoder;
  private OrderedListStateFetchIterator.State currentState;
  private ByteString continuationToken;
  private List<TimestampedValue<T>> next;
  private int currentIndex = -1;

  private enum State {
    READ_REQUIRED,
    HAS_NEXT,
    EOF
  }

  OrderedListStateFetchIterator(
      BeamFnStateClient beamFnStateClient,
      StateRequest stateRequest,
      OrderedListStateGetRequest getRequestForFirstChunk,
      Coder<T> valueCoder) {
    this.currentState = OrderedListStateFetchIterator.State.READ_REQUIRED;
    this.beamFnStateClient = beamFnStateClient;
    this.stateRequest = stateRequest;
    this.getRequestForFirstChunk = getRequestForFirstChunk;
    this.continuationToken = ByteString.EMPTY;
    this.valueCoder = valueCoder;
  }

  @Override
  public boolean hasNext() {
    if (next != null && currentIndex + 1 < next.size()) {
      currentIndex += 1;
      return true;
    }
    switch (currentState) {
      case EOF:
        return false;
      case READ_REQUIRED:
        CompletableFuture<StateResponse> stateResponseFuture = new CompletableFuture<>();
        beamFnStateClient.handle(
            stateRequest
                .toBuilder()
                .setOrderedListStateGet(
                    getRequestForFirstChunk
                        .toBuilder()
                        .setContinuationToken(continuationToken)
                        .build()),
            stateResponseFuture);
        StateResponse stateResponse;
        try {
          stateResponse = stateResponseFuture.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        } catch (ExecutionException e) {
          if (e.getCause() == null) {
            throw new IllegalStateException(e);
          }
          Throwables.throwIfUnchecked(e.getCause());
          throw new IllegalStateException(e.getCause());
        }
        continuationToken = stateResponse.getOrderedListStateGet().getContinuationToken();
        currentIndex = 0;
        currentState = State.HAS_NEXT;
        next =
            stateResponse.getOrderedListStateGet().getEntriesList().stream()
                .map(
                    entry -> {
                      try {
                        return TimestampedValue.of(
                            valueCoder.decode(entry.getData().newInput()),
                            new Instant(entry.getSortKey()));
                      } catch (IOException e) {
                        throw new IllegalStateException(e);
                      }
                    })
                .collect(Collectors.toList());
        return true;
      case HAS_NEXT:
        return true;
    }
    throw new IllegalStateException(String.format("Unknown state %s", currentState));
  }

  @Override
  public TimestampedValue<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    // If the continuation token is empty, that means we have reached EOF.
    currentState =
        ByteString.EMPTY.equals(continuationToken)
            ? OrderedListStateFetchIterator.State.EOF
            : OrderedListStateFetchIterator.State.READ_REQUIRED;
    return next.get(currentIndex);
  }
}
