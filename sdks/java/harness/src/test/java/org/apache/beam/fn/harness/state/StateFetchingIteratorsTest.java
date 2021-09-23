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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.fn.harness.state.StateFetchingIterators.FirstPageAndRemainder;
import org.apache.beam.fn.harness.state.StateFetchingIterators.LazyBlockingStateFetchingIterator;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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

    private BeamFnStateClient fakeStateClient(AtomicInteger callCount, ByteString... expected) {
      return (requestBuilder) -> {
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

  @RunWith(JUnit4.class)
  public static class FirstPageAndRemainderTest {

    @Test
    public void testEmptyValues() throws Exception {
      testFetchValues(VarIntCoder.of());
    }

    @Test
    public void testOneValue() throws Exception {
      testFetchValues(VarIntCoder.of(), 4);
    }

    @Test
    public void testManyValues() throws Exception {
      testFetchValues(VarIntCoder.of(), 1, 22, 333, 4444, 55555, 666666);
    }

    private <T> void testFetchValues(Coder<T> coder, T... expected) {
      List<ByteString> byteStrings =
          Arrays.stream(expected)
              .map(
                  value -> {
                    try {
                      return CoderUtils.encodeToByteArray(coder, value);
                    } catch (CoderException exn) {
                      throw new RuntimeException(exn);
                    }
                  })
              .map(ByteString::copyFrom)
              .collect(Collectors.toList());

      AtomicInteger callCount = new AtomicInteger();
      BeamFnStateClient fakeStateClient =
          fakeStateClient(callCount, Iterables.toArray(byteStrings, ByteString.class));
      PrefetchableIterable<T> values =
          new FirstPageAndRemainder<>(fakeStateClient, StateRequest.getDefaultInstance(), coder);

      // Ensure it's fully lazy.
      assertEquals(0, callCount.get());
      PrefetchableIterator<T> valuesIter = values.iterator();
      assertFalse(valuesIter.isReady());
      assertEquals(0, callCount.get());

      // Ensure that the first page result is cached across multiple iterators and subsequent
      // iterators are ready and prefetch does nothing
      valuesIter.prefetch();
      assertTrue(valuesIter.isReady());
      assertEquals(1, callCount.get());

      PrefetchableIterator<T> valuesIter2 = values.iterator();
      assertTrue(valuesIter2.isReady());
      valuesIter2.prefetch();
      assertEquals(1, callCount.get());

      // Prefetch every second element in the iterator capturing the results
      List<T> results = new ArrayList<>();
      for (int i = 0; i < expected.length; ++i) {
        if (i % 2 == 1) {
          // Ensure that prefetch performs the call
          valuesIter2.prefetch();
          assertTrue(valuesIter2.isReady());
          // Note that this is i+2 because we expect to prefetch the page after the current one
          // We also have to bound it to the max number of pages
          assertEquals(Math.min(i + 2, expected.length), callCount.get());
        }
        assertTrue(valuesIter2.hasNext());
        results.add(valuesIter2.next());
      }
      assertFalse(valuesIter2.hasNext());
      assertTrue(valuesIter2.isReady());

      // The contents agree.
      assertArrayEquals(expected, Iterables.toArray(values, Object.class));
    }
  }
}
