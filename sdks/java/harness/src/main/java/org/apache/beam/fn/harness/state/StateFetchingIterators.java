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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams.DataStreamDecoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.sdk.fn.stream.PrefetchableIterators;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;

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
  public static PrefetchableIterator<ByteString> readAllStartingFrom(
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
  public static <T> PrefetchableIterable<T> readAllAndDecodeStartingFrom(
      BeamFnStateClient beamFnStateClient,
      StateRequest stateRequestForFirstChunk,
      Coder<T> valueCoder) {
    return new FirstPageAndRemainder<>(beamFnStateClient, stateRequestForFirstChunk, valueCoder);
  }

  /**
   * A helper class that (lazily) gives the first page of a paginated state request separately from
   * all the remaining pages.
   */
  @VisibleForTesting
  static class FirstPageAndRemainder<T> implements PrefetchableIterable<T> {
    private final BeamFnStateClient beamFnStateClient;
    private final StateRequest stateRequestForFirstChunk;
    private final Coder<T> valueCoder;
    private LazyCachingIteratorToIterable<T> firstPage;
    private CompletableFuture<StateResponse> firstPageResponseFuture;
    private ByteString continuationToken;

    FirstPageAndRemainder(
        BeamFnStateClient beamFnStateClient,
        StateRequest stateRequestForFirstChunk,
        Coder<T> valueCoder) {
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestForFirstChunk = stateRequestForFirstChunk;
      this.valueCoder = valueCoder;
    }

    @Override
    public PrefetchableIterator<T> iterator() {
      return new PrefetchableIterator<T>() {
        PrefetchableIterator<T> delegate;

        private void ensureDelegateExists() {
          if (delegate == null) {
            // Fetch the first page if necessary
            prefetchFirstPage();
            if (firstPage == null) {
              StateResponse stateResponse;
              try {
                stateResponse = firstPageResponseFuture.get();
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
              firstPage =
                  new LazyCachingIteratorToIterable<>(
                      new DataStreamDecoder<>(
                          valueCoder,
                          PrefetchableIterators.fromArray(stateResponse.getGet().getData())));
            }

            if (ByteString.EMPTY.equals((continuationToken))) {
              delegate = firstPage.iterator();
            } else {
              delegate =
                  PrefetchableIterators.concat(
                      firstPage.iterator(),
                      new DataStreamDecoder<>(
                          valueCoder,
                          new LazyBlockingStateFetchingIterator(
                              beamFnStateClient,
                              stateRequestForFirstChunk
                                  .toBuilder()
                                  .setGet(
                                      StateGetRequest.newBuilder()
                                          .setContinuationToken(continuationToken))
                                  .build())));
            }
          }
        }

        @Override
        public boolean isReady() {
          if (delegate == null) {
            if (firstPageResponseFuture != null) {
              return firstPageResponseFuture.isDone();
            }
            return false;
          }
          return delegate.isReady();
        }

        @Override
        public void prefetch() {
          if (firstPageResponseFuture == null) {
            prefetchFirstPage();
          } else if (delegate != null && !delegate.isReady()) {
            delegate.prefetch();
          }
        }

        @Override
        public boolean hasNext() {
          if (delegate == null) {
            // Ensure that we prefetch the second page after the first has been accessed.
            // Prefetching subsequent pages after the first will be handled by the
            // LazyBlockingStateFetchingIterator
            ensureDelegateExists();
            boolean rval = delegate.hasNext();
            delegate.prefetch();
            return rval;
          }
          return delegate.hasNext();
        }

        @Override
        public T next() {
          if (delegate == null) {
            // Ensure that we prefetch the second page after the first has been accessed.
            // Prefetching subsequent pages after the first will be handled by the
            // LazyBlockingStateFetchingIterator
            ensureDelegateExists();
            T rval = delegate.next();
            delegate.prefetch();
            return rval;
          }
          return delegate.next();
        }
      };
    }

    private void prefetchFirstPage() {
      if (firstPageResponseFuture == null) {
        firstPageResponseFuture =
            beamFnStateClient.handle(
                stateRequestForFirstChunk.toBuilder().setGet(stateRequestForFirstChunk.getGet()));
      }
    }
  }

  /**
   * An {@link Iterator} which fetches {@link ByteString} chunks using the State API.
   *
   * <p>This iterator will only request a chunk on first access. Subsequently it eagerly pre-fetches
   * one future chunk at a time.
   */
  @VisibleForTesting
  static class LazyBlockingStateFetchingIterator implements PrefetchableIterator<ByteString> {

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
    private CompletableFuture<StateResponse> prefetchedResponse;

    LazyBlockingStateFetchingIterator(
        BeamFnStateClient beamFnStateClient, StateRequest stateRequestForFirstChunk) {
      this.currentState = State.READ_REQUIRED;
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestForFirstChunk = stateRequestForFirstChunk;
      this.continuationToken = stateRequestForFirstChunk.getGet().getContinuationToken();
    }

    @Override
    public boolean isReady() {
      if (prefetchedResponse == null) {
        return currentState != State.READ_REQUIRED;
      }
      return prefetchedResponse.isDone();
    }

    @Override
    public void prefetch() {
      if (currentState == State.READ_REQUIRED && prefetchedResponse == null) {
        prefetchedResponse =
            beamFnStateClient.handle(
                stateRequestForFirstChunk
                    .toBuilder()
                    .setGet(StateGetRequest.newBuilder().setContinuationToken(continuationToken)));
      }
    }

    @Override
    public boolean hasNext() {
      switch (currentState) {
        case EOF:
          return false;
        case READ_REQUIRED:
          prefetch();
          StateResponse stateResponse;
          try {
            stateResponse = prefetchedResponse.get();
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
          prefetchedResponse = null;
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
      if (ByteString.EMPTY.equals(continuationToken)) {
        currentState = State.EOF;
      } else {
        currentState = State.READ_REQUIRED;
        prefetch();
      }
      return next;
    }
  }
}
