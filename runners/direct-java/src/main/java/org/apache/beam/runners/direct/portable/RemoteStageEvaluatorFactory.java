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
package org.apache.beam.runners.direct.portable;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * The {@link TransformEvaluatorFactory} which produces {@link TransformEvaluator evaluators} for
 * stages which execute on an SDK harness via the Fn Execution APIs.
 */
class RemoteStageEvaluatorFactory implements TransformEvaluatorFactory {
  private final BundleFactory bundleFactory;

  private final JobBundleFactory jobFactory;

  private final EvaluationContext evaluationContext;

  RemoteStageEvaluatorFactory(
      BundleFactory bundleFactory,
      JobBundleFactory jobFactory,
      EvaluationContext evaluationContext) {
    this.bundleFactory = bundleFactory;
    this.jobFactory = jobFactory;
    this.evaluationContext = evaluationContext;
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) throws Exception {
    return new RemoteStageEvaluator<>(application, evaluationContext);
  }

  @Override
  public void cleanup() throws Exception {
    jobFactory.close();
  }

  private class RemoteStageEvaluator<T> implements TransformEvaluator<T> {
    private final PTransformNode transform;
    private final RemoteBundle bundle;
    private final FnDataReceiver<WindowedValue<?>> mainInput;
    private final Collection<UncommittedBundle<?>> outputs;

    private RemoteStageEvaluator(PTransformNode transform, EvaluationContext evaluationContext)
        throws Exception {
      this.transform = transform;
      ExecutableStage stage =
          ExecutableStage.fromPayload(
              ExecutableStagePayload.parseFrom(transform.getTransform().getSpec().getPayload()));
      this.outputs = new ArrayList<>();
      StageBundleFactory stageFactory = jobFactory.forStage(stage);

      StateRequestHandler handler =
          getStateRequestHandler(
              stage, stageFactory.getProcessBundleDescriptor(), evaluationContext);

      this.bundle =
          stageFactory.getBundle(
              BundleFactoryOutputReceiverFactory.create(
                  bundleFactory, stage.getComponents(), outputs::add),
              handler,
              BundleProgressHandler.ignored());

      // TODO(BEAM-4680): Add support for timers as inputs to the ULR
      this.mainInput = Iterables.getOnlyElement(bundle.getInputReceivers().values());
    }

    private StateRequestHandler getStateRequestHandler(
        ExecutableStage executableStage,
        ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor,
        EvaluationContext evaluationContext) {
      final StateRequestHandler sideInputHandler;
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          ReferenceSideInputHandlerFactory.forStage(executableStage, evaluationContext);
      try {
        sideInputHandler =
            StateRequestHandlers.forSideInputHandlerFactory(
                ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
      } catch (IOException e) {
        throw new RuntimeException("Failed to setup state handler for side input", e);
      }

      EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(StateKey.TypeCase.class);
      handlerMap.put(StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);

      return StateRequestHandlers.delegateBasedUponType(handlerMap);
    }

    @Override
    public void processElement(WindowedValue<T> element) throws Exception {
      mainInput.accept(element);
    }

    @Override
    public TransformResult<T> finishBundle() throws Exception {
      bundle.close();
      return StepTransformResult.<T>withoutHold(transform).addOutput(outputs).build();
    }
  }
}
