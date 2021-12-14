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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.fn.harness.state.StateFetchingIterators.LazyBlockingStateFetchingIterator;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StateFetchingIterators}. */
@RunWith(Enclosed.class)
public class StateFetchingIteratorsTest {

  private static BeamFnStateClient fakeStateClient(
      AtomicInteger callCount, ByteString... expected) {
    return requestBuilder -> {
      callCount.incrementAndGet();
      if (expected.length == 0) {
        return CompletableFuture.completedFuture(
            StateResponse.newBuilder()
                .setId(requestBuilder.getId())
                .setGet(StateGetResponse.newBuilder())
                .build());
      }

      ByteString continuationToken = requestBuilder.getGet().getContinuationToken();

      int requestedPosition = 0; // Default position is 0
      if (!ByteString.EMPTY.equals(continuationToken)) {
        requestedPosition = Integer.parseInt(continuationToken.toStringUtf8());
      }
      // Compute the new continuation token
      ByteString newContinuationToken = ByteString.EMPTY;
      if (requestedPosition != expected.length - 1) {
        newContinuationToken = ByteString.copyFromUtf8(Integer.toString(requestedPosition + 1));
      }
      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setId(requestBuilder.getId())
              .setGet(
                  StateGetResponse.newBuilder()
                      .setData(expected[requestedPosition])
                      .setContinuationToken(newContinuationToken))
              .build());
    };
  }

  /** Tests for {@link CachingStateIterable}. */
  @RunWith(JUnit4.class)
  public static class CachingStateIterableTest {

    @Test
    public void testEmpty() throws Exception {
      testFetch(4);
    }

    @Test
    public void testNonEmpty() throws Exception {
      testFetch(4, 0);
    }

    @Test
    public void testMultipleElementsPerChunk() throws Exception {
      testFetch(8, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testSingleElementPerChunk() throws Exception {
      testFetch(4, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testChunkSmallerThenElementSize() throws Exception {
      testFetch(3, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testChunkLargerThenElementSize() throws Exception {
      testFetch(5, 0, 1, 2, 3, 4, 5);
    }

    private void testFetch(int chunkSize, int... expected) throws Exception {
      StateRequest requestForFirstChunk =
          StateRequest.newBuilder()
              .setStateKey(
                  StateKey.newBuilder()
                      .setBagUserState(
                          StateKey.BagUserState.newBuilder()
                              .setTransformId("transformId")
                              .setUserStateId("stateId")
                              .setKey(ByteString.copyFromUtf8("key"))
                              .setWindow(ByteString.copyFromUtf8("window"))))
              .setGet(StateGetRequest.getDefaultInstance())
              .build();
      FakeBeamFnStateClient fakeStateClient =
          new FakeBeamFnStateClient(
              BigEndianIntegerCoder.of(),
              ImmutableMap.of(requestForFirstChunk.getStateKey(), Ints.asList(expected)),
              chunkSize);

      PrefetchableIterator<Integer> byteStrings =
          new CachingStateIterable<>(
                  Caches.eternal(),
                  fakeStateClient,
                  requestForFirstChunk,
                  BigEndianIntegerCoder.of())
              .iterator();

      assertEquals(0, fakeStateClient.getCallCount()); // Ensure it's fully lazy.
      assertFalse(byteStrings.isReady());

      List<Integer> results = new ArrayList<>();
      for (int i = 0; i < expected.length; ++i) {
        assertTrue(byteStrings.hasNext());
        results.add(byteStrings.next());
      }
      assertFalse(byteStrings.hasNext());
      assertTrue(byteStrings.isReady());

      assertEquals(Ints.asList(expected), results);
    }
  }

  /** Tests for {@link StateFetchingIterators.LazyBlockingStateFetchingIterator}. */
  @RunWith(JUnit4.class)
  public static class LazyBlockingStateFetchingIteratorTest {

    @Test
    public void testEmpty() throws Exception {
      testFetch(ByteString.EMPTY);
    }

    @Test
    public void testNonEmpty() throws Exception {
      testFetch(ByteString.copyFromUtf8("A"));
    }

    @Test
    public void testWithLastByteStringBeingEmpty() throws Exception {
      testFetch(ByteString.copyFromUtf8("A"), ByteString.EMPTY);
    }

    @Test
    public void testMulti() throws Exception {
      testFetch(ByteString.copyFromUtf8("BC"), ByteString.copyFromUtf8("DEF"));
    }

    @Test
    public void testMultiWithEmptyByteStrings() throws Exception {
      testFetch(
          ByteString.EMPTY,
          ByteString.copyFromUtf8("BC"),
          ByteString.EMPTY,
          ByteString.EMPTY,
          ByteString.copyFromUtf8("DEF"),
          ByteString.EMPTY);
    }

    @Test
    public void testPrefetchIgnoredWhenExistingPrefetchOngoing() throws Exception {
      AtomicInteger callCount = new AtomicInteger();
      BeamFnStateClient fakeStateClient =
          new BeamFnStateClient() {
            @Override
            public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
              callCount.incrementAndGet();
              return new CompletableFuture<StateResponse>();
            }
          };
      PrefetchableIterator<ByteString> byteStrings =
          new LazyBlockingStateFetchingIterator(
              Caches.noop(), fakeStateClient, StateRequest.getDefaultInstance());
      assertEquals(0, callCount.get());
      byteStrings.prefetch();
      assertEquals(1, callCount.get()); // first prefetch
      byteStrings.prefetch();
      assertEquals(1, callCount.get()); // subsequent is ignored
    }

    @Test
    public void testSeekToContinuationToken() throws Exception {
      BeamFnStateClient fakeStateClient =
          new BeamFnStateClient() {
            @Override
            public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
              int token = 0;
              if (!ByteString.EMPTY.equals(requestBuilder.getGet().getContinuationToken())) {
                token =
                    Integer.parseInt(requestBuilder.getGet().getContinuationToken().toStringUtf8());
              }
              return CompletableFuture.completedFuture(
                  StateResponse.newBuilder()
                      .setGet(
                          StateGetResponse.newBuilder()
                              .setData(ByteString.copyFromUtf8("value" + token))
                              .setContinuationToken(
                                  ByteString.copyFromUtf8(Integer.toString(token + 1))))
                      .build());
            }
          };
      LazyBlockingStateFetchingIterator byteStrings =
          new LazyBlockingStateFetchingIterator(
              Caches.noop(), fakeStateClient, StateRequest.getDefaultInstance());
      assertEquals(ByteString.copyFromUtf8("value" + 0), byteStrings.next());
      assertEquals(ByteString.copyFromUtf8("value" + 1), byteStrings.next());
      assertEquals(ByteString.copyFromUtf8("value" + 2), byteStrings.next());

      // Seek to the beginning
      byteStrings.seekToContinuationToken(ByteString.EMPTY);
      assertEquals(ByteString.copyFromUtf8("value" + 0), byteStrings.next());
      assertEquals(ByteString.copyFromUtf8("value" + 1), byteStrings.next());
      assertEquals(ByteString.copyFromUtf8("value" + 2), byteStrings.next());

      // Seek to an arbitrary offset
      byteStrings.seekToContinuationToken(ByteString.copyFromUtf8("42"));
      assertEquals(ByteString.copyFromUtf8("value" + 42), byteStrings.next());
      assertEquals(ByteString.copyFromUtf8("value" + 43), byteStrings.next());
      assertEquals(ByteString.copyFromUtf8("value" + 44), byteStrings.next());
    }

    private void testFetch(ByteString... expected) {
      AtomicInteger callCount = new AtomicInteger();
      BeamFnStateClient fakeStateClient = fakeStateClient(callCount, expected);
      PrefetchableIterator<ByteString> byteStrings =
          new LazyBlockingStateFetchingIterator(
              Caches.noop(), fakeStateClient, StateRequest.getDefaultInstance());
      assertEquals(0, callCount.get()); // Ensure it's fully lazy.
      assertFalse(byteStrings.isReady());

      // Prefetch every second element in the iterator capturing the results
      List<ByteString> results = new ArrayList<>();
      for (int i = 0; i < expected.length; ++i) {
        if (i % 2 == 0) {
          // Ensure that prefetch performs the call
          byteStrings.prefetch();
          assertEquals(i + 1, callCount.get());
          assertTrue(byteStrings.isReady());
        }
        assertTrue(byteStrings.hasNext());
        results.add(byteStrings.next());
      }
      assertFalse(byteStrings.hasNext());
      assertTrue(byteStrings.isReady());

      assertEquals(Arrays.asList(expected), results);
    }
  }
}
