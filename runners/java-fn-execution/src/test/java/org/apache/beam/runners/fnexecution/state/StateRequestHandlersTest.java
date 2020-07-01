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
package org.apache.beam.runners.fnexecution.state;

import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.EnumMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link StateRequestHandlers}. */
@RunWith(JUnit4.class)
public class StateRequestHandlersTest {
  @Test
  public void testDelegatingStateHandlerDelegates() throws Exception {
    StateRequestHandler mockHandler = Mockito.mock(StateRequestHandler.class);
    StateRequestHandler mockHandler2 = Mockito.mock(StateRequestHandler.class);
    EnumMap<StateKey.TypeCase, StateRequestHandler> handlers =
        new EnumMap<>(StateKey.TypeCase.class);
    handlers.put(StateKey.TypeCase.TYPE_NOT_SET, mockHandler);
    handlers.put(TypeCase.MULTIMAP_SIDE_INPUT, mockHandler2);
    StateRequest request = StateRequest.getDefaultInstance();
    StateRequest request2 =
        StateRequest.newBuilder()
            .setStateKey(
                StateKey.newBuilder().setMultimapSideInput(MultimapSideInput.getDefaultInstance()))
            .build();
    StateRequestHandlers.delegateBasedUponType(handlers).handle(request);
    StateRequestHandlers.delegateBasedUponType(handlers).handle(request2);
    verify(mockHandler).handle(request);
    verify(mockHandler2).handle(request2);
    verifyNoMoreInteractions(mockHandler, mockHandler2);
  }

  @Test
  public void testDelegatingStateHandlerThrowsWhenNotFound() throws Exception {
    StateRequestHandlers.delegateBasedUponType(new EnumMap<>(StateKey.TypeCase.class))
        .handle(StateRequest.getDefaultInstance());
  }

  @Test
  public void testUserStateCacheTokenGeneration() throws Exception {
    ExecutableStage stage = buildExecutableStage("state1", "state2");
    ProcessBundleDescriptors.ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "id", stage, Endpoints.ApiServiceDescriptor.getDefaultInstance());

    InMemoryBagUserStateFactory inMemoryBagUserStateFactory = new InMemoryBagUserStateFactory<>();
    assertThat(inMemoryBagUserStateFactory.handlers.size(), is(0));

    StateRequestHandler stateRequestHandler =
        StateRequestHandlers.forBagUserStateHandlerFactory(descriptor, inMemoryBagUserStateFactory);
    final BeamFnApi.ProcessBundleRequest.CacheToken cacheToken =
        assertSingleCacheToken(stateRequestHandler);

    sendGetRequest(stateRequestHandler, "state1");
    assertThat(inMemoryBagUserStateFactory.handlers.size(), is(1));
    assertThat(assertSingleCacheToken(stateRequestHandler), is(cacheToken));

    sendGetRequest(stateRequestHandler, "state2");
    assertThat(inMemoryBagUserStateFactory.handlers.size(), is(2));
    assertThat(assertSingleCacheToken(stateRequestHandler), is(cacheToken));
  }

  private static BeamFnApi.ProcessBundleRequest.CacheToken assertSingleCacheToken(
      StateRequestHandler stateRequestHandler) {
    Iterable<BeamFnApi.ProcessBundleRequest.CacheToken> cacheTokens =
        stateRequestHandler.getCacheTokens();
    assertThat(Iterables.size(cacheTokens), is(1));

    BeamFnApi.ProcessBundleRequest.CacheToken cacheToken = Iterables.getOnlyElement(cacheTokens);
    assertThat(cacheToken.getToken(), is(notNullValue()));
    assertThat(
        cacheToken.getUserState(),
        is(BeamFnApi.ProcessBundleRequest.CacheToken.UserState.getDefaultInstance()));
    return cacheToken;
  }

  private static void sendGetRequest(StateRequestHandler stateRequestHandler, String userStateName)
      throws Exception {
    stateRequestHandler
        .handle(
            StateRequest.newBuilder()
                .setGet(BeamFnApi.StateGetRequest.getDefaultInstance())
                .setStateKey(
                    StateKey.newBuilder()
                        .setBagUserState(
                            StateKey.BagUserState.newBuilder()
                                .setKey(ByteString.copyFromUtf8("key"))
                                .setWindow(
                                    ByteString.copyFrom(
                                        CoderUtils.encodeToByteArray(
                                            GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)))
                                .setTransformId("transform")
                                .setUserStateId(userStateName))
                        .build())
                .build())
        .toCompletableFuture()
        .get();
  }

  private static ExecutableStage buildExecutableStage(String... userStateNames) {
    RunnerApi.ExecutableStagePayload.Builder builder =
        RunnerApi.ExecutableStagePayload.newBuilder()
            .setInput("input")
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putWindowingStrategies(
                        "window",
                        RunnerApi.WindowingStrategy.newBuilder()
                            .setWindowCoderId("windowCoder")
                            .build())
                    .putPcollections(
                        "input",
                        RunnerApi.PCollection.newBuilder()
                            .setWindowingStrategyId("window")
                            .setCoderId("coder")
                            .build())
                    .putCoders(
                        "windowCoder",
                        RunnerApi.Coder.newBuilder()
                            .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                    .setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
                                    .build())
                            .build())
                    .putCoders(
                        "coder",
                        RunnerApi.Coder.newBuilder()
                            .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                    .setUrn(ModelCoders.KV_CODER_URN)
                                    .build())
                            .addComponentCoderIds("keyCoder")
                            .addComponentCoderIds("valueCoder")
                            .build())
                    .putCoders("keyCoder", RunnerApi.Coder.getDefaultInstance())
                    .putCoders("valueCoder", RunnerApi.Coder.getDefaultInstance())
                    .putTransforms(
                        "transform",
                        RunnerApi.PTransform.newBuilder()
                            .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                    .setUrn(PAR_DO_TRANSFORM_URN)
                                    .build())
                            .putInputs("input", "input")
                            .build())
                    .build());

    for (String userStateName : userStateNames) {
      builder.addUserStates(
          RunnerApi.ExecutableStagePayload.UserStateId.newBuilder()
              .setTransformId("transform")
              .setLocalName(userStateName)
              .build());
    }

    return ExecutableStage.fromPayload(builder.build());
  }
}
