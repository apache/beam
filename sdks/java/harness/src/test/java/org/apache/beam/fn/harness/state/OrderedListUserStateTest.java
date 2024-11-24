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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.OrderedListUserState.TimestampedValueCoder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListRange;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OrderedListUserStateTest {
  private static final TimestampedValue<String> A1 =
      TimestampedValue.of("A1", Instant.ofEpochMilli(1));
  private static final TimestampedValue<String> B1 =
      TimestampedValue.of("B1", Instant.ofEpochMilli(1));
  private static final TimestampedValue<String> C1 =
      TimestampedValue.of("C1", Instant.ofEpochMilli(1));
  private static final TimestampedValue<String> A2 =
      TimestampedValue.of("A2", Instant.ofEpochMilli(2));
  private static final TimestampedValue<String> B2 =
      TimestampedValue.of("B2", Instant.ofEpochMilli(2));
  private static final TimestampedValue<String> A3 =
      TimestampedValue.of("A3", Instant.ofEpochMilli(3));
  private static final TimestampedValue<String> A4 =
      TimestampedValue.of("A4", Instant.ofEpochMilli(4));

  private final String pTransformId = "pTransformId";
  private final String stateId = "stateId";
  private final String encodedWindow = "encodedWindow";
  private final Coder<TimestampedValue<String>> timestampedValueCoder =
      TimestampedValueCoder.of(StringUtf8Coder.of());

  @Test
  public void testNoPersistedValues() throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());
    assertThat(userState.read(), is(emptyIterable()));
  }

  @Test
  public void testRead() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(createOrderedListStateKey("A", 1), asList(A1, B1)));
    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    assertArrayEquals(
        asList(A1, B1).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.read());
  }

  @Test
  public void testReadRange() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1), asList(A1, B1),
                createOrderedListStateKey("A", 4), Collections.singletonList(A4),
                createOrderedListStateKey("A", 2), Collections.singletonList(A2)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    Iterable<TimestampedValue<String>> stateBeforeB2 =
        userState.readRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4));
    assertArrayEquals(
        Collections.singletonList(A2).toArray(),
        Iterables.toArray(stateBeforeB2, TimestampedValue.class));

    // Add a new value to an existing sort key
    userState.add(B2);
    assertArrayEquals(
        Collections.singletonList(A2).toArray(),
        Iterables.toArray(stateBeforeB2, TimestampedValue.class));
    assertArrayEquals(
        asList(A2, B2).toArray(),
        Iterables.toArray(
            userState.readRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4)),
            TimestampedValue.class));

    // Add a new value to a new sort key
    userState.add(A3);
    assertArrayEquals(
        Collections.singletonList(A2).toArray(),
        Iterables.toArray(stateBeforeB2, TimestampedValue.class));
    assertArrayEquals(
        asList(A2, B2, A3).toArray(),
        Iterables.toArray(
            userState.readRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4)),
            TimestampedValue.class));

    userState.asyncClose();
    assertThrows(
        IllegalStateException.class,
        () -> userState.readRange(Instant.ofEpochMilli(1), Instant.ofEpochMilli(2)));
  }

  @Test
  public void testAdd() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4),
                createOrderedListStateKey("A", 2),
                asList(A2, B2)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    // add to an existing timestamp
    userState.add(B1);
    assertArrayEquals(
        asList(A1, B1, A2, B2, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    // add to a nonexistent timestamp
    userState.add(A3);
    assertArrayEquals(
        asList(A1, B1, A2, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    // add a duplicated value
    userState.add(B1);
    assertArrayEquals(
        asList(A1, B1, B1, A2, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.add(A1));
  }

  @Test
  public void testClearRange() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                asList(A1, B1),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4),
                createOrderedListStateKey("A", 2),
                asList(A2, B2),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    Iterable<TimestampedValue<String>> initStateFrom2To3 =
        userState.readRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4));

    // clear range below the current timestamp range
    userState.clearRange(Instant.ofEpochMilli(-1), Instant.ofEpochMilli(0));
    assertArrayEquals(
        asList(A2, B2, A3).toArray(), Iterables.toArray(initStateFrom2To3, TimestampedValue.class));
    assertArrayEquals(
        asList(A1, B1, A2, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    // clear range above the current timestamp range
    userState.clearRange(Instant.ofEpochMilli(5), Instant.ofEpochMilli(10));
    assertArrayEquals(
        asList(A2, B2, A3).toArray(), Iterables.toArray(initStateFrom2To3, TimestampedValue.class));
    assertArrayEquals(
        asList(A1, B1, A2, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    // clear range that falls inside the current timestamp range
    userState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4));
    assertArrayEquals(
        asList(A2, B2, A3).toArray(), Iterables.toArray(initStateFrom2To3, TimestampedValue.class));
    assertArrayEquals(
        asList(A1, B1, A4).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));

    // clear range that partially covers the current timestamp range
    userState.clearRange(Instant.ofEpochMilli(3), Instant.ofEpochMilli(5));
    assertArrayEquals(
        asList(A2, B2, A3).toArray(), Iterables.toArray(initStateFrom2To3, TimestampedValue.class));
    assertArrayEquals(
        asList(A1, B1).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));

    // clear range that fully covers the current timestamp range
    userState.clearRange(Instant.ofEpochMilli(-1), Instant.ofEpochMilli(10));
    assertArrayEquals(
        asList(A2, B2, A3).toArray(), Iterables.toArray(initStateFrom2To3, TimestampedValue.class));
    assertThat(userState.read(), is(emptyIterable()));

    userState.asyncClose();
    assertThrows(
        IllegalStateException.class,
        () -> userState.clearRange(Instant.ofEpochMilli(1), Instant.ofEpochMilli(2)));
  }

  @Test
  public void testClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                asList(A1, B1),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4),
                createOrderedListStateKey("A", 2),
                asList(A2, B2),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    Iterable<TimestampedValue<String>> stateBeforeClear = userState.read();
    userState.clear();
    assertArrayEquals(
        asList(A1, B1, A2, B2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeClear, TimestampedValue.class));
    assertThat(userState.read(), is(emptyIterable()));

    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.clear());
  }

  @Test
  public void testAddAndClearRange() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    // add to a non-existing timestamp, clear, and then add
    userState.add(A2);
    Iterable<TimestampedValue<String>> stateBeforeFirstClearRange = userState.read();
    userState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(3));
    assertArrayEquals(
        asList(A1, A2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeFirstClearRange, TimestampedValue.class));
    assertArrayEquals(
        asList(A1, A3, A4).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));
    userState.add(B2);
    assertArrayEquals(
        asList(A1, A2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeFirstClearRange, TimestampedValue.class));
    assertArrayEquals(
        asList(A1, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    // add to an existing timestamp, clear, and then add
    userState.add(B1);
    userState.clearRange(Instant.ofEpochMilli(1), Instant.ofEpochMilli(2));
    assertArrayEquals(
        asList(A1, A2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeFirstClearRange, TimestampedValue.class));
    assertArrayEquals(
        asList(B2, A3, A4).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));
    userState.add(B1);
    assertArrayEquals(
        asList(A1, A2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeFirstClearRange, TimestampedValue.class));
    assertArrayEquals(
        asList(B1, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    // add a duplicated value, clear, and then add
    userState.add(A3);
    userState.clearRange(Instant.ofEpochMilli(3), Instant.ofEpochMilli(4));
    assertArrayEquals(
        asList(A1, A2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeFirstClearRange, TimestampedValue.class));
    assertArrayEquals(
        asList(B1, B2, A4).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));
    userState.add(A3);
    assertArrayEquals(
        asList(A1, A2, A3, A4).toArray(),
        Iterables.toArray(stateBeforeFirstClearRange, TimestampedValue.class));
    assertArrayEquals(
        asList(B1, B2, A3, A4).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));
  }

  @Test
  public void testAddAndClearRangeAfterClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    userState.clear();
    userState.clearRange(Instant.ofEpochMilli(0), Instant.ofEpochMilli(5));
    assertThat(userState.read(), is(emptyIterable()));

    userState.add(A1);
    assertArrayEquals(
        Collections.singletonList(A1).toArray(),
        Iterables.toArray(userState.read(), TimestampedValue.class));

    userState.add(A2);
    userState.add(A3);
    assertArrayEquals(
        asList(A1, A2, A3).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));

    userState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(3));
    assertArrayEquals(
        asList(A1, A3).toArray(), Iterables.toArray(userState.read(), TimestampedValue.class));
  }

  @Test
  public void testNoopAsyncCloseAndRead() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      userState.asyncClose();
    }

    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      assertArrayEquals(
          asList(A1, A3, A4).toArray(),
          Iterables.toArray(userState.read(), TimestampedValue.class));
    }
  }

  @Test
  public void testAddAsyncCloseAndRead() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      userState.add(B1);
      userState.add(A2);
      userState.asyncClose();
    }
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      assertArrayEquals(
          asList(A1, B1, A2, A3, A4).toArray(),
          Iterables.toArray(userState.read(), TimestampedValue.class));
    }
  }

  @Test
  public void testClearRangeAsyncCloseAndRead() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 2),
                Collections.singletonList(A2),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      userState.clearRange(Instant.ofEpochMilli(1), Instant.ofEpochMilli(3));
      userState.clearRange(Instant.ofEpochMilli(4), Instant.ofEpochMilli(5));
      userState.asyncClose();
    }
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      assertArrayEquals(
          Collections.singletonList(A3).toArray(),
          Iterables.toArray(userState.read(), TimestampedValue.class));
    }
  }

  @Test
  public void testAddClearRangeAsyncCloseAndRead() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      userState.add(B1);
      userState.add(A2);
      userState.add(A3);
      userState.clearRange(Instant.ofEpochMilli(1), Instant.ofEpochMilli(3));
      userState.clearRange(Instant.ofEpochMilli(4), Instant.ofEpochMilli(5));
      userState.asyncClose();
    }
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      assertArrayEquals(
          Collections.singletonList(A3).toArray(),
          Iterables.toArray(userState.read(), TimestampedValue.class));
    }
  }

  @Test
  public void testClearAsyncCloseAndRead() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                Collections.singletonList(A1),
                createOrderedListStateKey("A", 2),
                Collections.singletonList(A2),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      userState.clear();
      userState.asyncClose();
    }
    {
      OrderedListUserState<String> userState =
          new OrderedListUserState<>(
              Caches.noop(),
              fakeClient,
              "instructionId",
              createOrderedListStateKey("A"),
              StringUtf8Coder.of());

      assertThat(userState.read(), is(emptyIterable()));
    }
  }

  @Test
  public void testOperationsDuringNavigatingIterable() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            timestampedValueCoder,
            ImmutableMap.of(
                createOrderedListStateKey("A", 1),
                asList(A1, B1),
                createOrderedListStateKey("A", 2),
                asList(A2, B2),
                createOrderedListStateKey("A", 3),
                Collections.singletonList(A3),
                createOrderedListStateKey("A", 4),
                Collections.singletonList(A4)));

    OrderedListUserState<String> userState =
        new OrderedListUserState<>(
            Caches.noop(),
            fakeClient,
            "instructionId",
            createOrderedListStateKey("A"),
            StringUtf8Coder.of());

    Iterator<TimestampedValue<String>> iter = userState.read().iterator();
    assertEquals(iter.next(), A1);

    // Adding a C1 locally, but it should not be returned after B1 in the existing iterable.
    userState.add(C1);
    assertEquals(iter.next(), B1);
    assertEquals(iter.next(), A2);

    // Clearing range [2,4) locally, but B2 and A3 should still be returned.
    userState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4));
    assertEquals(iter.next(), B2);
    assertEquals(iter.next(), A3);

    // Clearing all ranges locally, but A4 should still be returned.
    userState.clear();
    assertEquals(iter.next(), A4);
  }

  private ByteString encode(String... values) throws IOException {
    ByteStringOutputStream out = new ByteStringOutputStream();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }

  private StateKey createOrderedListStateKey(String key) throws IOException {
    return StateKey.newBuilder()
        .setOrderedListUserState(
            StateKey.OrderedListUserState.newBuilder()
                .setWindow(encode(encodedWindow))
                .setTransformId(pTransformId)
                .setUserStateId(stateId)
                .setKey(encode(key)))
        .build();
  }

  private StateKey createOrderedListStateKey(String key, long sortKey) throws IOException {
    return StateKey.newBuilder()
        .setOrderedListUserState(
            StateKey.OrderedListUserState.newBuilder()
                .setWindow(encode(encodedWindow))
                .setTransformId(pTransformId)
                .setUserStateId(stateId)
                .setKey(encode(key))
                .setRange(
                    OrderedListRange.newBuilder().setStart(sortKey).setEnd(sortKey + 1).build()))
        .build();
  }
}
