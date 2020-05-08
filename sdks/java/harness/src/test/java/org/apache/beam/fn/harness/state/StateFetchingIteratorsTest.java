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

import java.util.Iterator;
import org.apache.beam.fn.harness.state.StateFetchingIterators.LazyBlockingStateFetchingIterator;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
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

    private void testFetch(ByteString... expected) {
      BeamFnStateClient fakeStateClient =
          (requestBuilder, response) -> {
            ByteString continuationToken = requestBuilder.getGet().getContinuationToken();

            int requestedPosition = 0; // Default position is 0
            if (!ByteString.EMPTY.equals(continuationToken)) {
              requestedPosition = Integer.parseInt(continuationToken.toStringUtf8());
            }

            // Compute the new continuation token
            ByteString newContinuationToken = ByteString.EMPTY;
            if (requestedPosition != expected.length - 1) {
              newContinuationToken =
                  ByteString.copyFromUtf8(Integer.toString(requestedPosition + 1));
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
      Iterator<ByteString> byteStrings =
          new LazyBlockingStateFetchingIterator(fakeStateClient, StateRequest.getDefaultInstance());
      assertArrayEquals(expected, Iterators.toArray(byteStrings, Object.class));
    }
  }
}
