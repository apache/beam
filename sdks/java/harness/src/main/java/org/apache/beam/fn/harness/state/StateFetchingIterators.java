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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * Adapters which convert a a logical series of chunks using continuation tokens over the Beam Fn
 * State API into an {@link Iterator} of {@link ByteString}s.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class StateFetchingIterators {

  // do not instantiate
  private StateFetchingIterators() {}

  /**
   * This adapter handles using the continuation token to provide iteration over all the chunks
   * returned by the Beam Fn State API using the supplied state client and state request for the
   * first chunk of the state stream.
   *
   * @param beamFnStateClient A client for handling state requests.
   * @param stateRequestForFirstChunk A fully populated state request for the first (and possibly
   *     only) chunk of a state stream. This state request will be populated with a continuation
   *     token to request further chunks of the stream if required.
   */
  public static Iterator<ByteString> readAllStartingFrom(
      BeamFnStateClient beamFnStateClient, StateRequest stateRequestForFirstChunk) {
    return new LazyBlockingStateFetchingIterator(beamFnStateClient, stateRequestForFirstChunk);
  }

  /**
   * This adapter handles using the continuation token to provide iteration over all the elements
   * returned by the Beam Fn State API using the supplied state client, state request for the first
   * chunk of the state stream, and a value decoder.
   *
   * <p>The first page, and only the first page, of the state request results is cached for
   * efficient re-iteration for small state requests while still allowing unboundedly large state
   * requests without unboundedly large memory consumption.
   *
   * @param beamFnStateClient A client for handling state requests.
   * @param stateRequestForFirstChunk A fully populated state request for the first (and possibly
   *     only) chunk of a state stream. This state request will be populated with a continuation
   *     token to request further chunks of the stream if required.
   * @param valueCoder A coder for decoding the state stream.
   */
  public static <T> Iterable<T> readAllAndDecodeStartingFrom(
      BeamFnStateClient beamFnStateClient,
      StateRequest stateRequestForFirstChunk,
      Coder<T> valueCoder) {
    FirstPageAndRemainder firstPageAndRemainder =
        new FirstPageAndRemainder(beamFnStateClient, stateRequestForFirstChunk);
    return Iterables.concat(
        new LazyCachingIteratorToIterable<T>(
            new DataStreams.DataStreamDecoder<>(
                valueCoder,
                DataStreams.inbound(
                    new LazySingletonIterator<>(firstPageAndRemainder::firstPage)))),
        () ->
            new DataStreams.DataStreamDecoder<>(
                valueCoder, DataStreams.inbound(firstPageAndRemainder.remainder())));
  }

  /** A iterable that contains a single element, provided by a Supplier which is invoked lazily. */
  static class LazySingletonIterator<T> implements Iterator<T> {

    private final Supplier<T> supplier;
    private boolean hasNext;

    private LazySingletonIterator(Supplier<T> supplier) {
      this.supplier = supplier;
      hasNext = true;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      hasNext = false;
      return supplier.get();
    }
  }

  /**
   * An helper class that (lazily) gives the first page of a paginated state request separately from
   * all the remaining pages.
   */
  static class FirstPageAndRemainder {
    private final BeamFnStateClient beamFnStateClient;
    private final StateRequest stateRequestForFirstChunk;
    private ByteString firstPage = null;
    private ByteString continuationToken;

    private FirstPageAndRemainder(
        BeamFnStateClient beamFnStateClient, StateRequest stateRequestForFirstChunk) {
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestForFirstChunk = stateRequestForFirstChunk;
    }

    public ByteString firstPage() {
      if (firstPage == null) {
        CompletableFuture<StateResponse> stateResponseFuture = new CompletableFuture<>();
        beamFnStateClient.handle(
            stateRequestForFirstChunk.toBuilder().setGet(stateRequestForFirstChunk.getGet()),
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
        firstPage = stateResponse.getGet().getData();
      }
      return firstPage;
    }

    public Iterator<ByteString> remainder() {
      firstPage();
      if (ByteString.EMPTY.equals(continuationToken)) {
        return Collections.emptyIterator();
      } else {
        return new LazyBlockingStateFetchingIterator(
            beamFnStateClient,
            stateRequestForFirstChunk
                .toBuilder()
                .setGet(StateGetRequest.newBuilder().setContinuationToken(continuationToken))
                .build());
      }
    }
  }

  /**
   * An {@link Iterator} which fetches {@link ByteString} chunks using the State API.
   *
   * <p>This iterator will only request a chunk on first access. Also it does not eagerly pre-fetch
   * any future chunks and blocks whenever required to fetch the next block.
   */
  static class LazyBlockingStateFetchingIterator implements Iterator<ByteString> {

    private enum State {
      READ_REQUIRED,
      HAS_NEXT,
      EOF
    }

    private final BeamFnStateClient beamFnStateClient;
    private final StateRequest stateRequestForFirstChunk;
    private State currentState;
    private ByteString continuationToken;
    private ByteString next;

    LazyBlockingStateFetchingIterator(
        BeamFnStateClient beamFnStateClient, StateRequest stateRequestForFirstChunk) {
      this.currentState = State.READ_REQUIRED;
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestForFirstChunk = stateRequestForFirstChunk;
      this.continuationToken = stateRequestForFirstChunk.getGet().getContinuationToken();
    }

    @Override
    public boolean hasNext() {
      switch (currentState) {
        case EOF:
          return false;
        case READ_REQUIRED:
          CompletableFuture<StateResponse> stateResponseFuture = new CompletableFuture<>();
          beamFnStateClient.handle(
              stateRequestForFirstChunk
                  .toBuilder()
                  .setGet(StateGetRequest.newBuilder().setContinuationToken(continuationToken)),
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
