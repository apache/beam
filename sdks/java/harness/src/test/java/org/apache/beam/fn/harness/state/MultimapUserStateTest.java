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
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MultimapUserState}.
 *
 * <p>It is important to use a key type where its coder is not {@link Coder#consistentWithEquals()}
 * to ensure that comparisons are performed using structural values instead of object equality
 * during testing.
 */
@RunWith(JUnit4.class)
public class MultimapUserStateTest {
  private static final byte[] A0 = "A0".getBytes(StandardCharsets.UTF_8);
  private static final byte[] A1 = "A1".getBytes(StandardCharsets.UTF_8);
  private static final byte[] A2 = "A2".getBytes(StandardCharsets.UTF_8);
  private static final byte[] A3 = "A3".getBytes(StandardCharsets.UTF_8);
  private final String pTransformId = "pTransformId";
  private final String stateId = "stateId";
  private final String encodedKey = "encodedKey";
  private final String encodedWindow = "encodedWindow";

  @Test
  public void testNoPersistedValues() throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    assertThat(userState.keys(), is(emptyIterable()));
  }

  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get(A1);
    userState.put(A1, "V3");
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertArrayEquals(
        new String[] {"V1", "V2", "V3"}, Iterables.toArray(userState.get(A1), String.class));
    assertArrayEquals(new String[] {}, Iterables.toArray(userState.get(A2), String.class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.get(A1));
  }

  @Test
  public void testClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get(A1);
    userState.clear();
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertThat(userState.get(A1), is(emptyIterable()));
    assertThat(userState.keys(), is(emptyIterable()));

    userState.put(A1, "V1");
    userState.clear();
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertThat(userState.get(A1), is(emptyIterable()));
    assertThat(userState.keys(), is(emptyIterable()));

    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.clear());
  }

  @Test
  public void testKeys() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    userState.put(A2, "V1");
    Iterable<byte[]> initKeys = userState.keys();
    userState.put(A3, "V1");
    userState.put(A1, "V3");
    assertArrayEquals(new byte[][] {A1, A2}, Iterables.toArray(initKeys, byte[].class));
    assertArrayEquals(new byte[][] {A1, A2, A3}, Iterables.toArray(userState.keys(), byte[].class));

    userState.clear();
    assertArrayEquals(new byte[][] {A1, A2}, Iterables.toArray(initKeys, byte[].class));
    assertArrayEquals(new byte[][] {}, Iterables.toArray(userState.keys(), byte[].class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.keys());
  }

  @Test
  public void testPut() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get(A1);
    userState.put(A1, "V3");
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertArrayEquals(
        new String[] {"V1", "V2", "V3"}, Iterables.toArray(userState.get(A1), String.class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.put(A1, "V2"));
  }

  @Test
  public void testPutAfterRemove() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A0),
                createMultimapValueStateKey(A0),
                encode("V1")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.remove(A0);
    userState.put(A0, "V2");
    assertArrayEquals(new String[] {"V2"}, Iterables.toArray(userState.get(A0), String.class));
    userState.asyncClose();
    Map<StateKey, ByteString> data = fakeClient.getData();
    assertEquals(encode("V2"), data.get(createMultimapValueStateKey(A0)));
  }

  @Test
  public void testPutAfterClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A0),
                createMultimapValueStateKey(A0),
                encode("V1")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.clear();
    userState.put(A0, "V2");
    assertArrayEquals(new String[] {"V2"}, Iterables.toArray(userState.get(A0), String.class));
  }

  @Test
  public void testRemoveBeforeClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A0),
                createMultimapValueStateKey(A0),
                encode("V1")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.remove(A0);
    userState.clear();
    userState.asyncClose();
    // Clear takes precedence over specific key remove
    assertThat(fakeClient.getCallCount(), is(1));
  }

  @Test
  public void testPutBeforeClear() throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.put(A0, "V0");
    userState.put(A1, "V1");
    Iterable<String> values = userState.get(A1); // fakeClient call = 1
    userState.clear(); // fakeClient call = 2
    assertArrayEquals(new String[] {"V1"}, Iterables.toArray(values, String.class));
    userState.asyncClose();
    // Clear takes precedence over puts
    assertThat(fakeClient.getCallCount(), is(2));
  }

  @Test
  public void testPutBeforeRemove() throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.put(A0, "V0");
    userState.put(A1, "V1");
    Iterable<String> values = userState.get(A1); // fakeClient call = 1
    userState.remove(A0); // fakeClient call = 2
    userState.remove(A1); // fakeClient call = 3
    assertArrayEquals(new String[] {"V1"}, Iterables.toArray(values, String.class));
    userState.asyncClose();
    assertThat(fakeClient.getCallCount(), is(3));
    assertNull(fakeClient.getData().get(createMultimapValueStateKey(A0)));
    assertNull(fakeClient.getData().get(createMultimapValueStateKey(A1)));
  }

  @Test
  public void testRemove() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get(A1);
    userState.put(A1, "V3");

    userState.remove(A1);
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertThat(userState.keys(), is(emptyIterable()));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.remove(A1));
  }

  @Test
  public void testImmutableKeys() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    Iterable<byte[]> keys = userState.keys();
    Iterator<byte[]> keysIterator = keys.iterator();
    keysIterator.next();
    assertThrows(UnsupportedOperationException.class, () -> keysIterator.remove());
  }

  @Test
  public void testImmutableValues() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    Iterable<String> values = userState.get(A1);
    assertThrows(
        UnsupportedOperationException.class,
        () -> Iterables.removeAll(values, Arrays.asList("V1")));
  }

  @Test
  public void testClearAsyncClose() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.clear();
    userState.asyncClose();
    Map<StateKey, ByteString> data = fakeClient.getData();
    assertEquals(1, data.size());
    assertNull(data.get(createMultimapKeyStateKey()));
  }

  @Test
  public void testNoopAsyncClose() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.keys());
    assertEquals(0, fakeClient.getCallCount());
  }

  @Test
  public void testAsyncClose() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A0, A1),
                createMultimapValueStateKey(A0),
                encode("V1"),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    userState.remove(A0);
    userState.put(A1, "V3");
    userState.put(A2, "V1");
    userState.put(A3, "V1");
    userState.remove(A3);
    userState.asyncClose();
    Map<StateKey, ByteString> data = fakeClient.getData();
    assertNull(data.get(createMultimapValueStateKey(A0)));
    assertEquals(encode("V1", "V2", "V3"), data.get(createMultimapValueStateKey(A1)));
    assertEquals(encode("V1"), data.get(createMultimapValueStateKey(A2)));
  }

  @Test
  public void testNullKeysAndValues() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            NullableCoder.of(ByteArrayCoder.of()),
            NullableCoder.of(StringUtf8Coder.of()));
    userState.put(null, null);
    userState.put(null, null);
    userState.put(null, "V1");
    assertArrayEquals(
        new String[] {null, null, "V1"}, Iterables.toArray(userState.get(null), String.class));
  }

  @Test
  public void testNegativeCache() throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());
    assertArrayEquals(new String[] {}, Iterables.toArray(userState.get(A1), String.class));
    assertArrayEquals(new String[] {}, Iterables.toArray(userState.get(A1), String.class));
    assertThat(fakeClient.getCallCount(), is(1));
  }

  @Test
  public void testGetValuesPrefetch() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    PrefetchableIterable<String> values = userState.get(A1);
    assertEquals(0, fakeClient.getCallCount());
    values.iterator().prefetch();
    assertEquals(1, fakeClient.getCallCount());
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(values, String.class));
    assertEquals(1, fakeClient.getCallCount());
  }

  @Test
  public void testGetKeysPrefetch() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    PrefetchableIterable<byte[]> keys = userState.keys();
    assertEquals(0, fakeClient.getCallCount());
    keys.iterator().prefetch();
    assertEquals(1, fakeClient.getCallCount());
    assertArrayEquals(new byte[][] {A1}, Iterables.toArray(keys, byte[].class));
    assertEquals(1, fakeClient.getCallCount());
  }

  @Test
  public void testPutKeysPrefetch() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    userState.put(A2, "V3");
    PrefetchableIterable<byte[]> keys = userState.keys();
    assertEquals(0, fakeClient.getCallCount());
    keys.iterator().prefetch();
    assertEquals(1, fakeClient.getCallCount());
    assertArrayEquals(new byte[][] {A1, A2}, Iterables.toArray(keys, byte[].class));
    assertEquals(1, fakeClient.getCallCount());
  }

  @Test
  public void testRemoveKeysPrefetch() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    userState.remove(A1);
    userState.put(A1, "V3");
    PrefetchableIterable<String> values = userState.get(A1);
    assertEquals(0, fakeClient.getCallCount());
    values.iterator().prefetch();
    // Removed keys don't require accessing the underlying persisted state
    assertEquals(0, fakeClient.getCallCount());
    assertArrayEquals(new String[] {"V3"}, Iterables.toArray(values, String.class));
    // Removed keys don't require accessing the underlying persisted state
    assertEquals(0, fakeClient.getCallCount());
  }

  @Test
  public void testClearPrefetch() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    userState.clear();
    userState.put(A2, "V3");
    PrefetchableIterable<byte[]> keys = userState.keys();
    assertEquals(0, fakeClient.getCallCount());
    keys.iterator().prefetch();
    // Cleared keys don't require accessing the underlying persisted state
    assertEquals(0, fakeClient.getCallCount());
    assertArrayEquals(new byte[][] {A2}, Iterables.toArray(keys, byte[].class));
    // Cleared keys don't require accessing the underlying persisted state
    assertEquals(0, fakeClient.getCallCount());
  }

  @Test
  public void testAppendValuesPrefetch() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode(A1),
                createMultimapValueStateKey(A1),
                encode("V1", "V2")));
    MultimapUserState<byte[], String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            ByteArrayCoder.of(),
            StringUtf8Coder.of());

    userState.put(A1, "V3");
    PrefetchableIterable<String> values = userState.get(A1);
    assertEquals(0, fakeClient.getCallCount());
    values.iterator().prefetch();
    assertEquals(1, fakeClient.getCallCount());
    assertArrayEquals(new String[] {"V1", "V2", "V3"}, Iterables.toArray(values, String.class));
    assertEquals(1, fakeClient.getCallCount());
  }

  private StateKey createMultimapKeyStateKey() throws IOException {
    return StateKey.newBuilder()
        .setMultimapKeysUserState(
            StateKey.MultimapKeysUserState.newBuilder()
                .setWindow(encode(encodedWindow))
                .setKey(encode(encodedKey))
                .setTransformId(pTransformId)
                .setUserStateId(stateId))
        .build();
  }

  private StateKey createMultimapValueStateKey(byte[] key) throws IOException {
    return StateKey.newBuilder()
        .setMultimapUserState(
            StateKey.MultimapUserState.newBuilder()
                .setTransformId(pTransformId)
                .setUserStateId(stateId)
                .setWindow(encode(encodedWindow))
                .setKey(encode(encodedKey))
                .setMapKey(encode(key)))
        .build();
  }

  private ByteString encode(String... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }

  private ByteString encode(byte[]... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (byte[] value : values) {
      ByteArrayCoder.of().encode(value, out);
    }
    return out.toByteString();
  }
}
