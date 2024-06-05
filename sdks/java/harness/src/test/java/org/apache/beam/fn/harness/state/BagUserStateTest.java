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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BagUserState}. */
@RunWith(JUnit4.class)
public class BagUserStateTest {
  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList("A1", "A2", "A3")));
    BagUserState<String> userState =
        new BagUserState<>(
            Caches.noop(), fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));

    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.get());
  }

  @Test
  public void testGetCached() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList("A1", "A2", "A3")));

    Cache<?, ?> cache = Caches.eternal();
    {
      // First user state populates the cache.
      BagUserState<String> userState =
          new BagUserState<>(cache, fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));
      userState.asyncClose();
    }

    {
      // The next user state will load all of its contents from the cache.
      BagUserState<String> userState =
          new BagUserState<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              key("A"),
              StringUtf8Coder.of());
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));
      userState.asyncClose();
    }
  }

  @Test
  public void testAppend() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList("A1")));
    BagUserState<String> userState =
        new BagUserState<>(
            Caches.noop(), fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
    userState.append("A2");
    Iterable<String> stateBeforeA3 = userState.get();
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(stateBeforeA3, String.class));
    userState.append("A3");
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(stateBeforeA3, String.class));
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));
    userState.asyncClose();

    assertEquals(encode("A1", "A2", "A3"), fakeClient.getData().get(key("A")));
    assertThrows(IllegalStateException.class, () -> userState.append("A4"));
  }

  @SuppressWarnings("InlineMeInliner")
  @Test
  public void testAppendBatchingLimit() throws Exception {
    String a1 = "A1";
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList(a1)));
    BagUserState<String> userState =
        new BagUserState<>(
            Caches.noop(), fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
    String a2 = Strings.repeat("A2", 2 * 1024 * 1024);
    userState.append(a2);
    String a3 = Strings.repeat("A3", 4 * 1024 * 1024);
    userState.append(a3);
    String a4 = "A4";
    userState.append(a4);
    String a5 = Strings.repeat("A5", 100);
    userState.append(a5);
    String a6 = Strings.repeat("A6", 11 * 1024 * 1024);
    userState.append(a6);
    String a7 = "A7";
    userState.append(a7);
    Iterable<String> stateBeforeA3 = userState.get();
    assertArrayEquals(
        new String[] {a1, a2, a3, a4, a5, a6, a7}, Iterables.toArray(stateBeforeA3, String.class));
    userState.asyncClose();

    assertEquals(encode(a1, a2, a3, a4, a5, a6, a7), fakeClient.getData().get(key("A")));
    assertTrue(encode(a2, a3).size() > BagUserState.BAG_APPEND_BATCHING_LIMIT);
    assertTrue(encode(a3, a4, a5).size() < BagUserState.BAG_APPEND_BATCHING_LIMIT);
    assertTrue(encode(a3, a4, a5, a6).size() > BagUserState.BAG_APPEND_BATCHING_LIMIT);
    assertTrue(encode(a6, a7).size() > BagUserState.BAG_APPEND_BATCHING_LIMIT);

    assertArrayEquals(
        new ByteString[] {encode(a1), encode(a2), encode(a3, a4, a5), encode(a6), encode(a7)},
        Iterables.toArray(fakeClient.getRawData().get(key("A")), ByteString.class));
  }

  @Test
  public void testAppendCached() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList("A1")));
    Cache<?, ?> cache = Caches.eternal();
    {
      // First user state populates the cache.
      BagUserState<String> userState =
          new BagUserState<>(cache, fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
      userState.append("A2");
      Iterable<String> stateBeforeA3 = userState.get();
      assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(stateBeforeA3, String.class));
      userState.append("A3");
      assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(stateBeforeA3, String.class));
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));
      userState.asyncClose();
    }

    {
      // The next user state will load all of its contents from the cache including the appends
      // persisted via asyncClose.
      BagUserState<String> userState =
          new BagUserState<>(
              cache,
              requestBuilder -> {
                if (requestBuilder.hasGet()) {
                  throw new IllegalStateException("Unexpected call for test.");
                }
                return fakeClient.handle(requestBuilder);
              },
              "instructionId",
              key("A"),
              StringUtf8Coder.of());
      userState.append("A4");
      Iterable<String> stateBeforeA5 = userState.get();
      assertArrayEquals(
          new String[] {"A1", "A2", "A3", "A4"}, Iterables.toArray(stateBeforeA5, String.class));
      userState.append("A5");
      assertArrayEquals(
          new String[] {"A1", "A2", "A3", "A4"}, Iterables.toArray(stateBeforeA5, String.class));
      assertArrayEquals(
          new String[] {"A1", "A2", "A3", "A4", "A5"},
          Iterables.toArray(userState.get(), String.class));
      userState.asyncClose();
    }
    assertEquals(encode("A1", "A2", "A3", "A4", "A5"), fakeClient.getData().get(key("A")));
  }

  @Test
  public void testClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList("A1", "A2", "A3")));
    BagUserState<String> userState =
        new BagUserState<>(
            Caches.noop(), fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));
    userState.clear();
    assertFalse(userState.get().iterator().hasNext());
    userState.append("A4");
    assertArrayEquals(new String[] {"A4"}, Iterables.toArray(userState.get(), String.class));
    userState.clear();
    assertFalse(userState.get().iterator().hasNext());
    userState.asyncClose();

    assertNull(fakeClient.getData().get(key("A")));
    assertThrows(IllegalStateException.class, () -> userState.clear());
  }

  @Test
  public void testClearCached() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            StringUtf8Coder.of(), ImmutableMap.of(key("A"), asList("A1", "A2", "A3")));

    Cache<?, ?> cache = Caches.eternal();
    {
      // First user state populates the cache.
      BagUserState<String> userState =
          new BagUserState<>(cache, fakeClient, "instructionId", key("A"), StringUtf8Coder.of());
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.get(), String.class));
      userState.clear();
      assertFalse(userState.get().iterator().hasNext());
      userState.append("A4");
      assertArrayEquals(new String[] {"A4"}, Iterables.toArray(userState.get(), String.class));
      userState.asyncClose();
    }

    {
      // The next user state will load all of its contents from the cache including the clear and
      // append persisted via asyncClose.
      BagUserState<String> userState =
          new BagUserState<>(
              cache,
              requestBuilder -> {
                if (requestBuilder.hasGet()) {
                  throw new IllegalStateException("Unexpected call for test.");
                }
                return fakeClient.handle(requestBuilder);
              },
              "instructionId",
              key("A"),
              StringUtf8Coder.of());
      assertArrayEquals(new String[] {"A4"}, Iterables.toArray(userState.get(), String.class));
      userState.clear();
      assertFalse(userState.get().iterator().hasNext());
      userState.asyncClose();
    }

    {
      // The next user state will load all of its contents from the cache including the clear
      // persisted via asyncClose.
      BagUserState<String> userState =
          new BagUserState<>(
              cache,
              requestBuilder -> {
                if (requestBuilder.hasGet()) {
                  throw new IllegalStateException("Unexpected call for test.");
                }
                return fakeClient.handle(requestBuilder);
              },
              "instructionId",
              key("A"),
              StringUtf8Coder.of());
      assertArrayEquals(new String[] {}, Iterables.toArray(userState.get(), String.class));
      userState.asyncClose();
    }
    assertNull(fakeClient.getData().get(key("A")));
  }

  private StateKey key(String id) throws IOException {
    return StateKey.newBuilder()
        .setBagUserState(
            StateKey.BagUserState.newBuilder()
                .setTransformId("ptransformId")
                .setUserStateId("stateId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow"))
                .setKey(encode(id)))
        .build();
  }

  private ByteString encode(String... values) throws IOException {
    ByteStringOutputStream out = new ByteStringOutputStream();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }
}
