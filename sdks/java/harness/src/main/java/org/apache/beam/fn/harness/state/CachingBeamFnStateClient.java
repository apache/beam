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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.IterableSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.MultimapKeysSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;

public class CachingBeamFnStateClient implements BeamFnStateClient {

  private final BeamFnStateClient beamFnStateClient;
  private final Cache<Pair<StateKey, ByteString>, StateGetResponse> stateCache;
  private Map<Pair<String, String>, ByteString> sideInputCacheTokens;
  private ByteString userStateToken;

  public CachingBeamFnStateClient(
      BeamFnStateClient beamFnStateClient,
      Cache<Pair<StateKey, ByteString>, StateGetResponse> stateCache,
      List<CacheToken> cacheTokenList) {
    this.beamFnStateClient = beamFnStateClient;
    this.stateCache = stateCache;
    this.sideInputCacheTokens = new HashMap<>();
    this.userStateToken = null;

    // Set up cache tokens
    for (BeamFnApi.ProcessBundleRequest.CacheToken token : cacheTokenList) {
      if (token.hasUserState()) {
        this.userStateToken = token.getToken();
      } else if (token.hasSideInput()) {
        Pair<String, String> sideInput =
            new ImmutablePair<>(
                token.getSideInput().getTransformId(), token.getSideInput().getSideInputId());
        sideInputCacheTokens.put(sideInput, token.getToken());
      }
    }
  }

  @Override
  public void handle(
      BeamFnApi.StateRequest.Builder requestBuilder,
      CompletableFuture<BeamFnApi.StateResponse> response) {

    StateRequest request = requestBuilder.build();
    StateKey stateKey = request.getStateKey();
    ByteString cacheToken = getCacheToken(stateKey);
    StateResponse.Builder responseBuilder = StateResponse.newBuilder();

    // If not cacheable proceed as normal
    if (cacheToken == null) {
      beamFnStateClient.handle(requestBuilder, response);
      return;
    }

    // Check if data is in the cache already
    StateGetResponse cachedFirstPage = null;
    Pair<StateKey, ByteString> cacheKey = new ImmutablePair<>(stateKey, cacheToken);
    cachedFirstPage = stateCache.getIfPresent(cacheKey);

    switch (request.getRequestCase()) {
      case GET:
        if (ByteString.EMPTY.equals(request.getGet().getContinuationToken())) {
          if (cachedFirstPage == null) {
            beamFnStateClient.handle(requestBuilder, response);
            try {
              stateCache.put(cacheKey, response.get().getGet());
            } catch (Exception e) {
              // response should already be completed exceptionally
              return;
            }
          } else {
            response.complete(
                responseBuilder.setId(requestBuilder.getId()).setGet(cachedFirstPage).build());
          }
        } else {
          beamFnStateClient.handle(requestBuilder, response);
        }

        return;

      case APPEND:
        if (cachedFirstPage == null) {
          CompletableFuture<StateResponse> responseFuture = new CompletableFuture<>();
          beamFnStateClient.handle(
              StateRequest.newBuilder()
                  .setStateKey(stateKey)
                  .setGet(StateGetRequest.getDefaultInstance()),
              responseFuture);
          try {
            cachedFirstPage = responseFuture.get().getGet();
            stateCache.put(cacheKey, cachedFirstPage);
          } catch (Exception e) {
            // If we can't cache the value just send append as before
            beamFnStateClient.handle(requestBuilder, response);
            return;
          }
        }

        if (ByteString.EMPTY.equals(cachedFirstPage.getContinuationToken())) {
          cachedFirstPage =
              StateGetResponse.newBuilder()
                  .setData(cachedFirstPage.getData().concat(requestBuilder.getAppend().getData()))
                  .setContinuationToken(ByteString.EMPTY)
                  .build();
          stateCache.put(cacheKey, cachedFirstPage);
        }

        beamFnStateClient.handle(requestBuilder, response);
        return;

      case CLEAR:
        stateCache.put(cacheKey, StateGetResponse.getDefaultInstance());
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
    } else {
      Pair<String, String> sideInput;
      if (stateKey.hasIterableSideInput()) {
        IterableSideInput iterableSideInput = stateKey.getIterableSideInput();
        sideInput =
            new ImmutablePair<>(
                iterableSideInput.getTransformId(), iterableSideInput.getSideInputId());
        return sideInputCacheTokens.get(sideInput);
      } else if (stateKey.hasMultimapSideInput()) {
        MultimapSideInput multimapSideInput = stateKey.getMultimapSideInput();
        sideInput =
            new ImmutablePair<>(
                multimapSideInput.getTransformId(), multimapSideInput.getSideInputId());
        return sideInputCacheTokens.get(sideInput);
      } else if (stateKey.hasMultimapKeysSideInput()) {
        MultimapKeysSideInput multimapKeysSideInput = stateKey.getMultimapKeysSideInput();
        sideInput =
            new ImmutablePair<>(
                multimapKeysSideInput.getTransformId(), multimapKeysSideInput.getSideInputId());
        return sideInputCacheTokens.get(sideInput);
      }
    }
    return null;
  }
}
