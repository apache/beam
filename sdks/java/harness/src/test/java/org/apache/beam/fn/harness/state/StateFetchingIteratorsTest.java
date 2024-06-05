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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable.Block;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable.Blocks;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable.BlocksPrefix;
import org.apache.beam.fn.harness.state.StateFetchingIterators.LazyBlockingStateFetchingIterator;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
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
      testFetchAndClear(4);
    }

    @Test
    public void testNonEmpty() throws Exception {
      testFetchAndClear(4, 0);
    }

    @Test
    public void testMultipleElementsPerChunk() throws Exception {
      testFetchAndClear(8, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testSingleElementPerChunk() throws Exception {
      testFetchAndClear(4, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testChunkSmallerThenElementSize() throws Exception {
      testFetchAndClear(3, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testChunkLargerThenElementSize() throws Exception {
      testFetchAndClear(5, 0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testAppend() throws Exception {
      int[] expected = new int[] {0, 1, 2, 3, 4, 5};
      CachingStateIterable<Integer> iterable;

      // Test on an iterable that was never accessed
      iterable = create(5, expected);
      iterable.append(Ints.asList(42, 43));
      // The append is dropped since the entire iterable isn't in memory so next time we iterate we
      // expect to be given what the runner tells us is there.
      verifyFetch(iterable.iterator(), expected);

      // Test on a partially accessed iterable
      iterable = create(5, expected);
      iterable.append(Ints.asList(42, 43));
      Boolean ignored = iterable.iterator().hasNext();
      // The append is dropped since the entire iterable isn't in memory so next time we iterate we
      // expect to be given what the runner tells us is there.
      verifyFetch(iterable.iterator(), expected);

      // Test on a fully accessed iterable
      iterable = create(5, expected);
      verifyFetch(iterable.iterator(), expected);
      iterable.append(Ints.asList(42, 43));
      // The append was persisted so we should see the two lists combined
      verifyFetch(iterable.iterator(), 0, 1, 2, 3, 4, 5, 42, 43);
    }

    @Test
    public void testRemove() throws Exception {
      int[] expected = new int[] {0, 1, 2, 3, 4, 5};
      Set<Object> toRemove = new HashSet<>();
      toRemove.add(BigEndianIntegerCoder.of().structuralValue(2));
      toRemove.add(BigEndianIntegerCoder.of().structuralValue(4));
      CachingStateIterable<Integer> iterable;

      // Test on an iterable that was never accessed
      iterable = create(5, expected);
      iterable.remove(toRemove);
      // The remove is dropped since the entire iterable isn't in memory so next time we iterate we
      // expect to be given what the runner tells us is there.
      verifyFetch(iterable.iterator(), expected);

      // Test on a partially accessed iterable
      iterable = create(5, expected);
      iterable.remove(toRemove);
      Boolean ignored = iterable.iterator().hasNext();
      // The remove is dropped since the entire iterable isn't in memory so next time we iterate we
      // expect to be given what the runner tells us is there.
      verifyFetch(iterable.iterator(), expected);

      // Test on a fully accessed iterable
      iterable = create(5, expected);
      verifyFetch(iterable.iterator(), expected);
      iterable.remove(toRemove);
      // The remove was persisted so we should see the elements removed
      verifyFetch(iterable.iterator(), 0, 1, 3, 5);
    }

    @Test
    public void testCacheEvictionOrphansIteratorAndAllowsForIteratorToRejoin() throws Exception {
      int[] expected = new int[] {0, 1, 2, 3, 4, 5};
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

      // Each integer is its own block
      FakeBeamFnStateClient fakeStateClient =
          new FakeBeamFnStateClient(
              BigEndianIntegerCoder.of(),
              ImmutableMap.of(requestForFirstChunk.getStateKey(), Ints.asList(expected)),
              4);

      Cache<StateFetchingIterators.IterableCacheKey, Blocks<Integer>> cache = Caches.eternal();
      CachingStateIterable<Integer> iterable =
          new CachingStateIterable<>(
              cache, fakeStateClient, requestForFirstChunk, BigEndianIntegerCoder.of());
      // Loads the entire iterable into memory
      verifyFetch(iterable.iterator(), expected);
      assertThat(
          cache.peek(StateFetchingIterators.IterableCacheKey.INSTANCE),
          is(instanceOf(BlocksPrefix.class)));

      int stateRequestCount = fakeStateClient.getCallCount();
      // Start an iterator that is reading fully from cache.
      PrefetchableIterator<Integer> iterator = iterable.iterator();
      assertEquals(0, (int) iterator.next());
      assertEquals(stateRequestCount, fakeStateClient.getCallCount());

      // Remove from the cache when it is partially done and show that state is required
      stateRequestCount = fakeStateClient.getCallCount();
      cache.remove(StateFetchingIterators.IterableCacheKey.INSTANCE);
      assertEquals(1, (int) iterator.next());
      stateRequestCount += 2; // The first to get 1 and the second to start the prefetch of 2
      assertEquals(stateRequestCount, fakeStateClient.getCallCount());
      stateRequestCount += 1; // Start the prefetch for 3
      assertEquals(2, (int) iterator.next());
      assertEquals(stateRequestCount, fakeStateClient.getCallCount());
      assertNull(cache.peek(StateFetchingIterators.IterableCacheKey.INSTANCE));

      // Start another iterator and ensure that the cache is populated and state is interacted with
      stateRequestCount = fakeStateClient.getCallCount();
      PrefetchableIterator<Integer> iterator2 = iterable.iterator();
      assertEquals(0, (int) iterator2.next());
      assertThat(
          cache.peek(StateFetchingIterators.IterableCacheKey.INSTANCE),
          is(instanceOf(BlocksPrefix.class)));
      assertTrue(stateRequestCount < fakeStateClient.getCallCount());

      // Have the second iterator surpass the first and show that the first iterator starts to
      // re-use the cache again.
      assertEquals(1, (int) iterator2.next());
      assertEquals(2, (int) iterator2.next());
      assertEquals(3, (int) iterator2.next());
      assertEquals(4, (int) iterator2.next());
      stateRequestCount = fakeStateClient.getCallCount();
      assertEquals(3, (int) iterator.next());
      assertEquals(stateRequestCount, fakeStateClient.getCallCount());
    }

    @Test
    public void testBlocksPrefixShrinkage() throws Exception {
      List<Block<String>> originalBlocks =
          Arrays.asList(
              Block.fromValues(Arrays.asList("A"), null),
              Block.fromValues(Arrays.asList("B"), null),
              Block.fromValues(Arrays.asList("C"), null),
              Block.fromValues(Arrays.asList("D"), null),
              Block.fromValues(Arrays.asList("E"), null),
              Block.fromValues(Arrays.asList("F"), null));
      BlocksPrefix<String> blocks = new BlocksPrefix<>(originalBlocks);
      BlocksPrefix<String> abcBlocks = blocks.shrink();
      BlocksPrefix<String> aBlocks = abcBlocks.shrink();

      assertThat(
          abcBlocks.getBlocks(),
          contains(originalBlocks.get(0), originalBlocks.get(1), originalBlocks.get(2)));
      assertThat(aBlocks.getBlocks(), contains(originalBlocks.get(0)));
      assertNull(aBlocks.shrink());
    }

    @Test
    public void testBlocksWeight() throws Exception {
      List<Block<String>> originalBlocks =
          Arrays.asList(
              Block.mutatedBlock(Arrays.asList("A"), 10),
              Block.mutatedBlock(Arrays.asList("B"), Long.MAX_VALUE / 2),
              Block.mutatedBlock(Arrays.asList("C"), Long.MAX_VALUE / 2),
              Block.mutatedBlock(Arrays.asList("D"), 5));
      BlocksPrefix<String> blocks = new BlocksPrefix<>(originalBlocks.subList(0, 2));
      assertEquals(10 + Long.MAX_VALUE / 2, blocks.getWeight());

      BlocksPrefix<String> blocksOverflow = new BlocksPrefix<>(originalBlocks);
      assertEquals(Long.MAX_VALUE, blocksOverflow.getWeight());
    }

    private CachingStateIterable<Integer> create(int chunkSize, int... values) {
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
              ImmutableMap.of(requestForFirstChunk.getStateKey(), Ints.asList(values)),
              chunkSize);

      CachingStateIterable<Integer> iterable =
          new CachingStateIterable<>(
              Caches.eternal(), fakeStateClient, requestForFirstChunk, BigEndianIntegerCoder.of());

      Iterator<?> ignored = iterable.iterator();
      assertEquals(0, fakeStateClient.getCallCount()); // Ensure it's fully lazy.
      return iterable;
    }

    private void testFetchAndClear(int chunkSize, int... expected) throws Exception {
      PrefetchableIterator<Integer> iterator = create(chunkSize, expected).iterator();
      assertFalse(iterator.isReady());
      verifyFetch(iterator, expected);

      verifyClear(chunkSize, expected);
    }

    private void verifyFetch(PrefetchableIterator<Integer> iterator, int... expected) {
      List<Integer> results = new ArrayList<>();
      for (int i = 0; i < expected.length; ++i) {
        assertTrue(iterator.hasNext());
        results.add(iterator.next());
      }
      assertFalse(iterator.hasNext());
      assertTrue(iterator.isReady());

      assertEquals(Ints.asList(expected), results);
    }

    private void verifyClear(int chunkSize, int... expected) throws Exception {
      CachingStateIterable<Integer> iterable;

      // Verify an iterable that was never accessed
      iterable = create(chunkSize, expected);
      iterable.clearAndAppend(Ints.asList(42, 43));
      assertTrue(iterable.iterator().isReady());
      verifyFetch(iterable.iterator(), 42, 43);

      // Verify an iterable that was partially accessed
      iterable = create(chunkSize, expected);
      Boolean ignored = iterable.iterator().hasNext();
      iterable.clearAndAppend(Ints.asList(42, 43));
      assertTrue(iterable.iterator().isReady());
      verifyFetch(iterable.iterator(), 42, 43);

      // Verify an iterable that was fully accessed
      iterable = create(chunkSize, expected);
      verifyFetch(iterable.iterator(), expected);
      iterable.clearAndAppend(Ints.asList(42, 43));
      assertTrue(iterable.iterator().isReady());
      verifyFetch(iterable.iterator(), 42, 43);
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
          new LazyBlockingStateFetchingIterator(fakeStateClient, StateRequest.getDefaultInstance());
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
          new LazyBlockingStateFetchingIterator(fakeStateClient, StateRequest.getDefaultInstance());
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
          new LazyBlockingStateFetchingIterator(fakeStateClient, StateRequest.getDefaultInstance());
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
