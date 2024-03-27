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
import java.nio.charset.StandardCharsets;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MultimapSideInput}.
 *
 * <p>It is important to use a key type where its coder is not {@link Coder#consistentWithEquals()}
 * to ensure that comparisons are performed using structural values instead of object equality
 * during testing.
 */
@RunWith(JUnit4.class)
public class MultimapSideInputTest {
  private static final byte[] A = "A".getBytes(StandardCharsets.UTF_8);
  private static final byte[] B = "B".getBytes(StandardCharsets.UTF_8);
  private static final byte[] UNKNOWN = "UNKNOWN".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testGetWithBulkRead() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysValuesStateKey(),
                KV.of(
                    KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of())),
                    asList(KV.of(A, asList("A1", "A2", "A3")), KV.of(B, asList("B1", "B2"))))));

    MultimapSideInput<byte[], String> multimapSideInput =
        new MultimapSideInput<>(
            Caches.noop(),
            fakeBeamFnStateClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(multimapSideInput.get(A), String.class));
    assertArrayEquals(
        new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
    assertArrayEquals(
        new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
  }

  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysStateKey(), KV.of(ByteArrayCoder.of(), asList(A, B)),
                key(A), KV.of(StringUtf8Coder.of(), asList("A1", "A2", "A3")),
                key(B), KV.of(StringUtf8Coder.of(), asList("B1", "B2"))));

    MultimapSideInput<byte[], String> multimapSideInput =
        new MultimapSideInput<>(
            Caches.noop(),
            fakeBeamFnStateClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(multimapSideInput.get(A), String.class));
    assertArrayEquals(
        new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
    assertArrayEquals(
        new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
    assertArrayEquals(
        new byte[][] {A, B}, Iterables.toArray(multimapSideInput.get(), byte[].class));
  }

  @Test
  public void testGetCached() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysStateKey(), KV.of(ByteArrayCoder.of(), asList(A, B)),
                key(A), KV.of(StringUtf8Coder.of(), asList("A1", "A2", "A3")),
                key(B), KV.of(StringUtf8Coder.of(), asList("B1", "B2"))));

    Cache<?, ?> cache = Caches.eternal();
    {
      // The first side input will populate the cache.
      MultimapSideInput<byte[], String> multimapSideInput =
          new MultimapSideInput<>(
              cache,
              fakeBeamFnStateClient,
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"},
          Iterables.toArray(multimapSideInput.get(A), String.class));
      assertArrayEquals(
          new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
      assertArrayEquals(
          new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
      assertArrayEquals(
          new byte[][] {A, B}, Iterables.toArray(multimapSideInput.get(), byte[].class));
    }

    {
      // The next side input will load all of its contents from the cache.
      MultimapSideInput<byte[], String> multimapSideInput =
          new MultimapSideInput<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"},
          Iterables.toArray(multimapSideInput.get(A), String.class));
      assertArrayEquals(
          new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
      assertArrayEquals(
          new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
      assertArrayEquals(
          new byte[][] {A, B}, Iterables.toArray(multimapSideInput.get(), byte[].class));
    }
  }

  private StateKey keysStateKey() throws IOException {
    return StateKey.newBuilder()
        .setMultimapKeysSideInput(
            StateKey.MultimapKeysSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow")))
        .build();
  }

  private StateKey keysValuesStateKey() throws IOException {
    return StateKey.newBuilder()
        .setMultimapKeysValuesSideInput(
            StateKey.MultimapKeysValuesSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow")))
        .build();
  }

  private StateKey key(byte[] key) throws IOException {
    ByteStringOutputStream out = new ByteStringOutputStream();
    ByteArrayCoder.of().encode(key, out);
    return StateKey.newBuilder()
        .setMultimapSideInput(
            StateKey.MultimapSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow"))
                .setKey(out.toByteString()))
        .build();
  }
}
