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
package org.apache.beam.runners.samza.runtime;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.StreamingSideInputHandlerFactory;
import org.apache.beam.runners.fnexecution.wire.ByteStringCoder;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.util.StateUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.samza.context.TaskContext;

/**
 * This class creates {@link StateRequestHandler} for side inputs and states of the Samza portable
 * runner.
 */
public class SamzaStateRequestHandlers {

  public static StateRequestHandler of(
      String transformId,
      TaskContext context,
      SamzaPipelineOptions pipelineOptions,
      ExecutableStage executableStage,
      StageBundleFactory stageBundleFactory,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds,
      SideInputHandler sideInputHandler) {
    final StateRequestHandler sideInputStateHandler =
        createSideInputStateHandler(executableStage, sideInputIds, sideInputHandler);
    final StateRequestHandler userStateRequestHandler =
        createUserStateRequestHandler(
            transformId, executableStage, context, pipelineOptions, stageBundleFactory);
    final EnumMap<BeamFnApi.StateKey.TypeCase, StateRequestHandler> handlerMap =
        new EnumMap<>(BeamFnApi.StateKey.TypeCase.class);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.ITERABLE_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_KEYS_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.BAG_USER_STATE, userStateRequestHandler);
    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  private static StateRequestHandler createSideInputStateHandler(
      ExecutableStage executableStage,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds,
      SideInputHandler sideInputHandler) {

    if (executableStage.getSideInputs().size() <= 0) {
      return StateRequestHandler.unsupported();
    }

    final StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
        Preconditions.checkNotNull(
            StreamingSideInputHandlerFactory.forStage(
                executableStage, sideInputIds, sideInputHandler));
    try {
      return StateRequestHandlers.forSideInputHandlerFactory(
          ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize SideInputHandler", e);
    }
  }

  private static StateRequestHandler createUserStateRequestHandler(
      String transformId,
      ExecutableStage executableStage,
      TaskContext context,
      SamzaPipelineOptions pipelineOptions,
      StageBundleFactory stageBundleFactory) {

    if (!StateUtils.isStateful(executableStage)) {
      return StateRequestHandler.unsupported();
    }

    final SamzaStoreStateInternals.Factory<ByteString> stateInternalsFactory =
        SamzaStoreStateInternals.createStateInternalsFactory(
            transformId, ByteStringCoder.of(), context, pipelineOptions, executableStage);

    return StateRequestHandlers.forBagUserStateHandlerFactory(
        stageBundleFactory.getProcessBundleDescriptor(),
        new BagUserStateFactory<>(stateInternalsFactory));
  }

  /**
   * Factory to create {@link StateRequestHandlers.BagUserStateHandler} to provide bag state access
   * for the given {@link K key} and {@link W window} provided by SDK worker, unlike classic
   * pipeline where {@link K key} is set at {@link DoFnRunnerWithKeyedInternals#processElement} and
   * {@link W window} is set at {@link
   * org.apache.beam.runners.core.SimpleDoFnRunner.DoFnProcessContext#window()}}.
   */
  static class BagUserStateFactory<
          K extends ByteString, V extends ByteString, W extends BoundedWindow>
      implements StateRequestHandlers.BagUserStateHandlerFactory<K, V, W> {

    private final SamzaStoreStateInternals.Factory<K> stateInternalsFactory;

    BagUserStateFactory(SamzaStoreStateInternals.Factory<K> stateInternalsFactory) {
      this.stateInternalsFactory = stateInternalsFactory;
    }

    @Override
    public StateRequestHandlers.BagUserStateHandler<K, V, W> forUserState(
        String pTransformId,
        String userStateId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      return new StateRequestHandlers.BagUserStateHandler<K, V, W>() {

        /** {@inheritDoc} */
        @Override
        public Iterable<V> get(K key, W window) {
          StateNamespace namespace = StateNamespaces.window(windowCoder, window);
          BagState<V> bagState =
              stateInternalsFactory
                  .stateInternalsForKey(key)
                  .state(namespace, StateTags.bag(userStateId, valueCoder));
          return bagState.read();
        }

        /** {@inheritDoc} */
        @Override
        public void append(K key, W window, Iterator<V> values) {
          StateNamespace namespace = StateNamespaces.window(windowCoder, window);
          BagState<V> bagState =
              stateInternalsFactory
                  .stateInternalsForKey(key)
                  .state(namespace, StateTags.bag(userStateId, valueCoder));
          while (values.hasNext()) {
            bagState.add(values.next());
          }
        }

        /** {@inheritDoc} */
        @Override
        public void clear(K key, W window) {
          StateNamespace namespace = StateNamespaces.window(windowCoder, window);
          BagState<V> bagState =
              stateInternalsFactory
                  .stateInternalsForKey(key)
                  .state(namespace, StateTags.bag(userStateId, valueCoder));
          bagState.clear();
        }
      };
    }
  }
}
