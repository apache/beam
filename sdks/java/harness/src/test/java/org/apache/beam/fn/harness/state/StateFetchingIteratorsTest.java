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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.fn.harness.state.StateFetchingIterators.LazyBlockingStateFetchingIterator;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StateFetchingIterators}. */
public class StateFetchingIteratorsTest {
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
      return (requestBuilder, response) -> {
        callCount.incrementAndGet();
        if (expected.length == 0) {
          response.complete(
              StateResponse.newBuilder()
                  .setId(requestBuilder.getId())
                  .setGet(StateGetResponse.newBuilder())
                  .build());
          return;
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
        response.complete(
            StateResponse.newBuilder()
                .setId(requestBuilder.getId())
                .setGet(
                    StateGetResponse.newBuilder()
                        .setData(expected[requestedPosition])
                        .setContinuationToken(newContinuationToken))
                .build());
      };
    }

    private void testFetch(ByteString... expected) {
      AtomicInteger callCount = new AtomicInteger();
      BeamFnStateClient fakeStateClient = fakeStateClient(callCount, expected);
      Iterator<ByteString> byteStrings =
          new LazyBlockingStateFetchingIterator(fakeStateClient, StateRequest.getDefaultInstance());
      assertEquals(0, callCount.get()); // Ensure it's fully lazy.
      assertArrayEquals(expected, Iterators.toArray(byteStrings, Object.class));
    }

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
      testFetchValues(VarIntCoder.of(), 11, 37, 389, 5077);
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
      Iterable<T> values =
          StateFetchingIterators.readAllAndDecodeStartingFrom(
              fakeStateClient, StateRequest.getDefaultInstance(), coder);

      // Ensure it's fully lazy.
      assertEquals(0, callCount.get());
      Iterator<T> valuesIter = values.iterator();
      assertEquals(0, callCount.get());

      // No more is read than necissary.
      if (valuesIter.hasNext()) {
        valuesIter.next();
      }
      assertEquals(1, callCount.get());

      // The first page is cached.
      Iterator<T> valuesIter2 = values.iterator();
      assertEquals(1, callCount.get());
      if (valuesIter2.hasNext()) {
        valuesIter2.next();
      }
      assertEquals(1, callCount.get());

      // The contents agree.
      assertArrayEquals(expected, Iterables.toArray(values, Object.class));
    }
  }
}
