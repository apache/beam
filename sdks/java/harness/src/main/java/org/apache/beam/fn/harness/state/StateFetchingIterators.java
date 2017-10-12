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

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;

/**
 * Adapters which convert a a logical series of chunks using continuation tokens over the Beam
 * Fn State API into an {@link Iterator} of {@link ByteString}s.
 */
public class StateFetchingIterators {

  // do not instantiate
  private StateFetchingIterators() {}

  /**
   * This adapter handles using the continuation token to provide iteration over all the chunks
   * returned by the Beam Fn State API using the supplied state client and partially filled
   * out state request containing a state key.
   *
   * @param beamFnStateClient A client for handling state requests.
   * @param partialStateRequestBuilder A {@link StateRequest} with the
   * {@link StateRequest#getStateKey()} already set.
   * @return An {@code Iterator<ByteString>} representing all the requested data.
   */
  public static Iterator<ByteString> usingPartialRequestWithStateKey(
      BeamFnStateClient beamFnStateClient,
      Supplier<StateRequest.Builder> partialStateRequestBuilder) {
    return new LazyBlockingStateFetchingIterator(beamFnStateClient, partialStateRequestBuilder);
  }

  /**
   * An {@link Iterator} which fetches {@link ByteString} chunks using the State API.
   *
   * <p>This iterator will only request a chunk on first access. Also it does not eagerly
   * pre-fetch any future chunks and blocks whenever required to fetch the next block.
   */
  static class LazyBlockingStateFetchingIterator implements Iterator<ByteString> {
    private enum State { READ_REQUIRED, HAS_NEXT, EOF };
    private final BeamFnStateClient beamFnStateClient;
    /** Allows for the partially built state request to be memoized across many requests. */
    private final Supplier<Builder> stateRequestSupplier;
    private State currentState;
    private ByteString continuationToken;
    private ByteString next;

    LazyBlockingStateFetchingIterator(
        BeamFnStateClient beamFnStateClient,
        Supplier<StateRequest.Builder> stateRequestSupplier) {
      this.currentState = State.READ_REQUIRED;
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestSupplier = stateRequestSupplier;
      this.continuationToken = ByteString.EMPTY;
    }

    @Override
    public boolean hasNext() {
      switch (currentState) {
        case EOF:
          return false;
        case READ_REQUIRED:
          CompletableFuture<StateResponse> stateResponseFuture = new CompletableFuture<>();
          beamFnStateClient.handle(
              stateRequestSupplier.get().setGet(
                  StateGetRequest.newBuilder().setContinuationToken(continuationToken)),
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
          continuationToken = stateResponse.getGet().getContinuationToken();
          next = stateResponse.getGet().getData();
          currentState = State.HAS_NEXT;
          return true;
        case HAS_NEXT:
          return true;
      }
      throw new IllegalStateException(String.format("Unknown state %s", currentState));
    }

    @Override
    public ByteString next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      // If the continuation token is empty, that means we have reached EOF.
      currentState = ByteString.EMPTY.equals(continuationToken) ? State.EOF : State.READ_REQUIRED;
      return next;
    }
  }

}
