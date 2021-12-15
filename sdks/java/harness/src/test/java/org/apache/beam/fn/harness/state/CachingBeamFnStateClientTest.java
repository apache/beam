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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.KV;
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
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testNoCacheWithoutToken() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(key("A"), KV.of(StringUtf8Coder.of(), asList("A1", "A2", "A3"))));

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    StateRequest.Builder request =
        StateRequest.newBuilder()
            .setStateKey(key("A"))
            .setGet(BeamFnApi.StateGetRequest.newBuilder().build());

    cachingClient.handle(request);
    assertEquals(1, fakeClient.getCallCount());
    request.clearId();

    cachingClient.handle(request);
    assertEquals(2, fakeClient.getCallCount());
  }

  @Test
  public void testCachingUserState() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(key("A"), KV.of(StringUtf8Coder.of(), asList("A1", "A2", "A3"))), 3);

    cacheTokenList.add(userStateToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    assertEquals(fakeClient.getData().get(key("A")), getALlDataForKey(key("A"), cachingClient));
    assertEquals(3, fakeClient.getCallCount());

    assertEquals(fakeClient.getData().get(key("A")), getALlDataForKey(key("A"), cachingClient));
    assertEquals(3, fakeClient.getCallCount());
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
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                iterableSideInput, KV.of(StringUtf8Coder.of(), asList("S1", "S2", "S3"))),
            3);

    CacheToken iterableToken = sideInputCacheToken("GBK", "Iterable");
    cacheTokenList.add(iterableToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    assertEquals(
        fakeClient.getData().get(iterableSideInput),
        getALlDataForKey(iterableSideInput, cachingClient));
    assertEquals(3, fakeClient.getCallCount());

    assertEquals(
        fakeClient.getData().get(iterableSideInput),
        getALlDataForKey(iterableSideInput, cachingClient));
    assertEquals(3, fakeClient.getCallCount());
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

    Map<StateKey, KV<Coder<?>, List<?>>> clientData = new HashMap<>();
    clientData.put(multimapKeys, KV.of(StringUtf8Coder.of(), asList("K1", "K2")));
    clientData.put(multimapValues, KV.of(StringUtf8Coder.of(), asList("V1", "V2", "V3")));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(clientData, 3);

    CacheToken multimapToken = sideInputCacheToken("GBK", "Multimap");
    cacheTokenList.add(multimapToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    assertEquals(
        fakeClient.getData().get(multimapKeys), getALlDataForKey(multimapKeys, cachingClient));
    assertEquals(2, fakeClient.getCallCount());

    assertEquals(
        fakeClient.getData().get(multimapKeys), getALlDataForKey(multimapKeys, cachingClient));
    assertEquals(2, fakeClient.getCallCount());

    assertEquals(
        fakeClient.getData().get(multimapValues), getALlDataForKey(multimapValues, cachingClient));
    assertEquals(5, fakeClient.getCallCount());

    assertEquals(
        fakeClient.getData().get(multimapValues), getALlDataForKey(multimapValues, cachingClient));
    assertEquals(5, fakeClient.getCallCount());
  }

  @Test
  public void testAppendInvalidatesLastPage() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                key("A"),
                KV.of(StringUtf8Coder.of(), asList("A1")),
                key("B"),
                KV.of(StringUtf8Coder.of(), asList("B1"))),
            3);

    cacheTokenList.add(userStateToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    // Append works with no pages in cache
    appendToKey(key("A"), encode("A2"), cachingClient);
    assertTrue(stateCache.getUnchecked(key("A")).isEmpty());
    assertEquals(fakeClient.getData().get(key("A")), getALlDataForKey(key("A"), cachingClient));
    assertEquals(3, fakeClient.getCallCount());

    // Append works with multiple pages in cache
    appendToKey(key("A"), encode("A3"), cachingClient);
    assertFalse(
        stateCache
            .getUnchecked(key("A"))
            .containsValue(StateGetResponse.newBuilder().setData(encode("A2")).build()));
    assertEquals(fakeClient.getData().get(key("A")), getALlDataForKey(key("A"), cachingClient));
    assertEquals(6, fakeClient.getCallCount());

    // Append works with one page in the cache
    assertEquals(fakeClient.getData().get(key("B")), getALlDataForKey(key("B"), cachingClient));
    appendToKey(key("B"), encode("B2"), cachingClient);
    assertTrue(stateCache.getUnchecked(key("B")).isEmpty());
    assertEquals(fakeClient.getData().get(key("B")), getALlDataForKey(key("B"), cachingClient));
    assertEquals(10, fakeClient.getCallCount());

    // Append works with no prior data
    appendToKey(key("C"), encode("C1"), cachingClient);
    assertTrue(stateCache.getUnchecked(key("C")).isEmpty());
    assertEquals(fakeClient.getData().get(key("C")), getALlDataForKey(key("C"), cachingClient));
  }

  @Test
  public void testCacheClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                key("A"),
                KV.of(StringUtf8Coder.of(), asList("A1")),
                key("B"),
                KV.of(StringUtf8Coder.of(), asList("B1", "B2"))),
            3);

    cacheTokenList.add(userStateToken);

    CachingBeamFnStateClient cachingClient =
        new CachingBeamFnStateClient(fakeClient, stateCache, cacheTokenList);

    // Test clear cacheable value
    clearKey(key("A"), cachingClient);
    assertEquals(1, fakeClient.getCallCount());
    assertNull(fakeClient.getData().get(key("A")));
    assertEquals(ByteString.EMPTY, getALlDataForKey(key("A"), cachingClient));
    assertEquals(1, fakeClient.getCallCount());

    // Test clear cached value
    getALlDataForKey(key("B"), cachingClient);
    clearKey(key("B"), cachingClient);
    assertEquals(4, fakeClient.getCallCount());
    assertNull(fakeClient.getData().get(key("B")));
    assertEquals(ByteString.EMPTY, getALlDataForKey(key("B"), cachingClient));
    assertEquals(4, fakeClient.getCallCount());
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

  private void appendToKey(StateKey key, ByteString data, CachingBeamFnStateClient cachingClient)
      throws Exception {
    StateRequest.Builder appendRequestBuilder =
        StateRequest.newBuilder()
            .setStateKey(key)
            .setAppend(StateAppendRequest.newBuilder().setData(data));

    CompletableFuture<StateResponse> appendResponse = cachingClient.handle(appendRequestBuilder);
    appendResponse.get();
  }

  private void clearKey(StateKey key, CachingBeamFnStateClient cachingClient) throws Exception {
    StateRequest.Builder clearRequestBuilder =
        StateRequest.newBuilder().setStateKey(key).setClear(StateClearRequest.getDefaultInstance());

    CompletableFuture<StateResponse> clearResponse = cachingClient.handle(clearRequestBuilder);
    clearResponse.get();
  }

  private ByteString getALlDataForKey(StateKey key, CachingBeamFnStateClient cachingClient)
      throws Exception {
    ByteString continuationToken = ByteString.EMPTY;
    ByteString allData = ByteString.EMPTY;
    StateRequest.Builder requestBuilder = StateRequest.newBuilder().setStateKey(key);
    do {
      requestBuilder
          .clearId()
          .setGet(StateGetRequest.newBuilder().setContinuationToken(continuationToken));
      CompletableFuture<StateResponse> getResponse = cachingClient.handle(requestBuilder);
      continuationToken = getResponse.get().getGet().getContinuationToken();
      allData = allData.concat(getResponse.get().getGet().getData());
    } while (!continuationToken.equals(ByteString.EMPTY));

    return allData;
  }
}
