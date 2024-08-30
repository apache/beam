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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IterableSideInputTest {
  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(key(), asList("A1", "A2", "A3", "A4", "A5", "A6")));

    IterableSideInput<String> iterableSideInput =
        new IterableSideInput<>(
            Caches.noop(), fakeBeamFnStateClient, "instructionId", key(), StringUtf8Coder.of());
    assertArrayEquals(
        new String[] {"A1", "A2", "A3", "A4", "A5", "A6"},
        Iterables.toArray(iterableSideInput.get(), String.class));
  }

  @Test
  public void testGetCached() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(),
            ImmutableMap.of(key(), asList("A1", "A2", "A3", "A4", "A5", "A6")));

    Cache<?, ?> cache = Caches.eternal();
    {
      // The first side input will populate the cache.
      IterableSideInput<String> iterableSideInput =
          new IterableSideInput<>(
              cache, fakeBeamFnStateClient, "instructionId", key(), StringUtf8Coder.of());
      assertArrayEquals(
          new String[] {"A1", "A2", "A3", "A4", "A5", "A6"},
          Iterables.toArray(iterableSideInput.get(), String.class));
    }

    {
      // The next side input will load all of its contents from the cache.
      IterableSideInput<String> iterableSideInput =
          new IterableSideInput<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              key(),
              StringUtf8Coder.of());
      assertArrayEquals(
          new String[] {"A1", "A2", "A3", "A4", "A5", "A6"},
          Iterables.toArray(iterableSideInput.get(), String.class));
    }
  }

  private StateKey key() throws IOException {
    return StateKey.newBuilder()
        .setIterableSideInput(
            StateKey.IterableSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow")))
        .build();
  }
}
