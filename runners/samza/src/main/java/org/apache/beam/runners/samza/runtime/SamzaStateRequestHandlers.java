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
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.StreamingSideInputHandlerFactory;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * This class creates {@link StateRequestHandler} for side inputs and states of the Samza portable
 * runner.
 */
public class SamzaStateRequestHandlers {

  // TODO: [BEAM-12403] support state handlers
  public static StateRequestHandler of(
      ExecutableStage executableStage,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds,
      SideInputHandler sideInputHandler) {
    final StateRequestHandler sideInputStateHandler;
    if (executableStage.getSideInputs().size() > 0) {
      final StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          Preconditions.checkNotNull(
              StreamingSideInputHandlerFactory.forStage(
                  executableStage, sideInputIds, sideInputHandler));
      try {
        sideInputStateHandler =
            StateRequestHandlers.forSideInputHandlerFactory(
                ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize SideInputHandler", e);
      }
    } else {
      sideInputStateHandler = StateRequestHandler.unsupported();
    }

    final EnumMap<BeamFnApi.StateKey.TypeCase, StateRequestHandler> handlerMap =
        new EnumMap<>(BeamFnApi.StateKey.TypeCase.class);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.ITERABLE_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_KEYS_SIDE_INPUT, sideInputStateHandler);
    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }
}
