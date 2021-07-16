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

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.IterableSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.MultimapKeysSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;

/**
 * Wraps a delegate BeamFnStateClient and stores the result of state requests in cross bundle cache
 * according to the available cache tokens. If there are no cache tokens for the state key requested
 * the request is forwarded to the client and executed normally.
 */
public class CachingBeamFnStateClient implements BeamFnStateClient {

  private final BeamFnStateClient beamFnStateClient;
  private final LoadingCache<StateKey, Map<StateCacheKey, StateGetResponse>> stateCache;
  private final Map<CacheToken.SideInput, ByteString> sideInputCacheTokens;
  private final ByteString userStateToken;

  public CachingBeamFnStateClient(
      BeamFnStateClient beamFnStateClient,
      LoadingCache<StateKey, Map<StateCacheKey, StateGetResponse>> stateCache,
      List<CacheToken> cacheTokenList) {
    this.beamFnStateClient = beamFnStateClient;
    this.stateCache = stateCache;
    this.sideInputCacheTokens = new HashMap<>();

    // Set up cache tokens
    ByteString tempUserStateToken = ByteString.EMPTY;
    for (BeamFnApi.ProcessBundleRequest.CacheToken token : cacheTokenList) {
      if (token.hasUserState()) {
        tempUserStateToken = token.getToken();
      } else if (token.hasSideInput()) {
        sideInputCacheTokens.put(token.getSideInput(), token.getToken());
      }
    }

    this.userStateToken = tempUserStateToken;
  }

  @Override
  public void handle(
      StateRequest.Builder requestBuilder, CompletableFuture<StateResponse> response) {

    StateRequest request = requestBuilder.build();
    StateKey stateKey = request.getStateKey();
    ByteString cacheToken = getCacheToken(stateKey);

    // If not cacheable proceed as normal
    if (ByteString.EMPTY.equals(cacheToken)) {
      beamFnStateClient.handle(requestBuilder, response);
      return;
    }

    switch (request.getRequestCase()) {
      case GET:
        // Check if data is in the cache already
        StateGetResponse cachedPage;
        StateCacheKey cacheKey =
            StateCacheKey.create(cacheToken, request.getGet().getContinuationToken());
        Map<StateCacheKey, StateGetResponse> stateKeyMap = stateCache.getUnchecked(stateKey);
        cachedPage = stateKeyMap.get(cacheKey);

        if (cachedPage == null) {
          beamFnStateClient.handle(requestBuilder, response);
          CompletableFuture<Void> callback =
              response.thenAccept(
                  stateResponse -> {
                    stateCache.getUnchecked(stateKey).put(cacheKey, stateResponse.getGet());
                  });

          callback.getNow(null);
        } else {
          response.complete(
              StateResponse.newBuilder().setId(requestBuilder.getId()).setGet(cachedPage).build());
        }

        return;

      case APPEND:
        // Treat appends as normal for now and do not cache
        beamFnStateClient.handle(requestBuilder, response);
        return;

      case CLEAR:
        Map<StateCacheKey, StateGetResponse> clearedData = new HashMap<>();
        StateCacheKey newKey = StateCacheKey.create(cacheToken, ByteString.EMPTY);
        clearedData.put(newKey, StateGetResponse.getDefaultInstance());
        stateCache.put(stateKey, clearedData);
        beamFnStateClient.handle(requestBuilder, response);
        return;

      default:
        throw new IllegalStateException(
            String.format("Unknown request type %s", request.getRequestCase()));
    }
  }

  private ByteString getCacheToken(BeamFnApi.StateKey stateKey) {
    // type: (beam_fn_api_pb2.StateKey) -> Optional[bytes]
    if (stateKey.hasBagUserState()) {
      return userStateToken;
    } else if (stateKey.hasRunner()) {
      // TODO: Support runner state key caching
      return ByteString.EMPTY;
    } else {
      CacheToken.SideInput.Builder sideInputBuilder = CacheToken.SideInput.newBuilder();
      if (stateKey.hasIterableSideInput()) {
        IterableSideInput iterableSideInput = stateKey.getIterableSideInput();
        sideInputBuilder
            .setTransformId(iterableSideInput.getTransformId())
            .setSideInputId(iterableSideInput.getSideInputId());
      } else if (stateKey.hasMultimapSideInput()) {
        MultimapSideInput multimapSideInput = stateKey.getMultimapSideInput();
        sideInputBuilder
            .setTransformId(multimapSideInput.getTransformId())
            .setSideInputId(multimapSideInput.getSideInputId());
      } else if (stateKey.hasMultimapKeysSideInput()) {
        MultimapKeysSideInput multimapKeysSideInput = stateKey.getMultimapKeysSideInput();
        sideInputBuilder
            .setTransformId(multimapKeysSideInput.getTransformId())
            .setSideInputId(multimapKeysSideInput.getSideInputId());
      }
      return sideInputCacheTokens.getOrDefault(sideInputBuilder.build(), ByteString.EMPTY);
    }
  }

  @AutoValue
  public abstract static class StateCacheKey {
    public abstract ByteString getCacheToken();

    public abstract ByteString getContinuationToken();

    static StateCacheKey create(ByteString cacheToken, ByteString continuationToken) {
      return new AutoValue_CachingBeamFnStateClient_StateCacheKey(cacheToken, continuationToken);
    }
  }
}
