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
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.fn.harness.state.CachingBeamFnStateClient.StateCacheKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CachingBeamFnStateClient}. */
@RunWith(JUnit4.class)
public class CachingBeamFnStateClientTest {

  private LoadingCache<StateKey, Map<StateCacheKey, StateGetResponse>> stateCache;
  private List<CacheToken> cacheTokenList;
  private CacheToken userStateToken =
      CacheToken.newBuilder()
          .setUserState(CacheToken.UserState.getDefaultInstance())
          .setToken(ByteString.copyFromUtf8("1"))
          .build();
  private StateCacheKey defaultCacheKey =
      StateCacheKey.create(ByteString.copyFromUtf8("1"), ByteString.EMPTY);
  private CacheLoader<StateKey, Map<StateCacheKey, StateGetResponse>> loader =
      new CacheLoader<StateKey, Map<StateCacheKey, StateGetResponse>>() {
        @Override
        public Map<StateCacheKey, StateGetResponse> load(StateKey key) {
          return new HashMap<>();
        }
      };

  @Before
  public void setup() {
    stateCache = CacheBuilder.newBuilder().build(loader);
    cacheTokenList = new ArrayList<>();
  }

  @Test
  public void testNoCacheWithoutToken() throws Exception {

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(ImmutableMap.of(key("A"), encode("A1", "A2", "A3")), 1);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    CompletableFuture<BeamFnApi.StateResponse> response1 = new CompletableFuture<>();
    CompletableFuture<BeamFnApi.StateResponse> response2 = new CompletableFuture<>();

    StateRequest.Builder request =
        StateRequest.newBuilder()
            .setStateKey(key("A"))
            .setGet(BeamFnApi.StateGetRequest.newBuilder().build());

    cachingClient.handle(request, response1);
    assertEquals("1", response1.get().getId());
    request.clearId();

    cachingClient.handle(request, response2);
    assertEquals("2", response2.get().getId());
  }

  @Test
  public void testCachingUserState() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(ImmutableMap.of(key("A"), encode("A1")));

    cacheTokenList.add(userStateToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    testGetCaching(key("A"), defaultCacheKey, cachingClient, fakeClient);
  }

  @Test
  public void testCachingIterableSideInput() throws Exception {
    StateKey iterableSideInput =
        StateKey.newBuilder()
            .setIterableSideInput(
                StateKey.IterableSideInput.newBuilder()
                    .setTransformId("GBK")
                    .setSideInputId("Iterable")
                    .build())
            .build();

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(ImmutableMap.of(iterableSideInput, encode("S1", "S2")));

    CacheToken iterableToken = sideInputCacheToken("GBK", "Iterable");
    cacheTokenList.add(iterableToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    testGetCaching(iterableSideInput, defaultCacheKey, cachingClient, fakeClient);
  }

  @Test
  public void testCachingMultimapSideInput() throws Exception {

    StateKey multimapKeys =
        StateKey.newBuilder()
            .setMultimapKeysSideInput(
                StateKey.MultimapKeysSideInput.newBuilder()
                    .setTransformId("GBK")
                    .setSideInputId("Multimap")
                    .build())
            .build();

    StateKey multimapValues =
        StateKey.newBuilder()
            .setMultimapSideInput(
                StateKey.MultimapSideInput.newBuilder()
                    .setTransformId("GBK")
                    .setSideInputId("Multimap")
                    .setKey(encode("K1"))
                    .build())
            .build();

    Map<StateKey, ByteString> clientData = new HashMap<>();
    clientData.put(multimapKeys, encode("K1", "K2"));
    clientData.put(multimapValues, encode("V1"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(clientData);

    CacheToken multimapToken = sideInputCacheToken("GBK", "Multimap");
    cacheTokenList.add(multimapToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    testGetCaching(multimapKeys, defaultCacheKey, cachingClient, fakeClient);
    testGetCaching(multimapValues, defaultCacheKey, cachingClient, fakeClient);
  }

  @Test
  public void testCacheOnAppendOrClear() throws Exception {

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(ImmutableMap.of(key("A"), encode("A1")));

    cacheTokenList.add(userStateToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    StateRequest.Builder requestBuilder =
        StateRequest.newBuilder().setAppend(StateAppendRequest.newBuilder().setData(encode("V")));

    // Test append to empty cacheable value
    testMutationCaching(requestBuilder, key("B"), defaultCacheKey, cachingClient, fakeClient);

    // Test append to non empty cacheable value
    testMutationCaching(requestBuilder, key("A"), defaultCacheKey, cachingClient, fakeClient);

    // Test clear
    requestBuilder.clearAppend().setClear(StateClearRequest.getDefaultInstance());

    // Test clear cacheable value
    testMutationCaching(requestBuilder, key("C"), defaultCacheKey, cachingClient, fakeClient);

    // Test clear cached value
    testMutationCaching(requestBuilder, key("A"), defaultCacheKey, cachingClient, fakeClient);
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

  private CacheToken sideInputCacheToken(String transformID, String sideInputID) {
    return CacheToken.newBuilder()
        .setSideInput(
            CacheToken.SideInput.newBuilder()
                .setTransformId(transformID)
                .setSideInputId(sideInputID)
                .build())
        .setToken(ByteString.copyFromUtf8("1"))
        .build();
  }

  private ByteString encode(String... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }

  private void testGetCaching(
      StateKey stateKey,
      StateCacheKey cacheKey,
      BeamFnStateClient cachingClient,
      FakeBeamFnStateClient fakeClient)
      throws Exception {
    CompletableFuture<StateResponse> firstResponse = new CompletableFuture<>();
    CompletableFuture<StateResponse> cachedResponse = new CompletableFuture<>();

    StateRequest.Builder requestBuilder =
        StateRequest.newBuilder()
            .setStateKey(stateKey)
            .setGet(
                StateGetRequest.newBuilder().setContinuationToken(cacheKey.getContinuationToken()));

    cachingClient.handle(requestBuilder, firstResponse);
    assertEquals(firstResponse.get().getGet(), stateCache.getIfPresent(stateKey).get(cacheKey));
    requestBuilder.clearId();

    cachingClient.handle(requestBuilder, cachedResponse);
    assertEquals("", cachedResponse.get().getId());
    assertEquals(firstResponse.get().getGet(), cachedResponse.get().getGet());
  }

  private void testMutationCaching(
      StateRequest.Builder requestBuilder,
      StateKey stateKey,
      StateCacheKey cacheKey,
      BeamFnStateClient cachingClient,
      FakeBeamFnStateClient fakeClient)
      throws Exception {
    requestBuilder.setStateKey(stateKey);
    requestBuilder.clearId();
    CompletableFuture<StateResponse> stateResponseFuture = new CompletableFuture<>();
    cachingClient.handle(requestBuilder, stateResponseFuture);
    if (requestBuilder.hasAppend()) {
      // Include more testing after append is implemented
    } else {
      assertEquals(
          StateGetResponse.getDefaultInstance(), stateCache.getIfPresent(stateKey).get(cacheKey));
      assertNull(fakeClient.getData().get(stateKey));
    }
  }
}
