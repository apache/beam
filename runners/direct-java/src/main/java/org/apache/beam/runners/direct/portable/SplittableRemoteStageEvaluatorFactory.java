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

import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.splittabledofn.SDFFeederViaStateAndTimers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

/**
 * The {@link TransformEvaluatorFactory} for {@link #URN}, which reads from a {@link
 * DirectGroupByKey#DIRECT_GBKO_URN} and feeds the data, using state and timers, to a {@link
 * ExecutableStage} whose first instruction is an SDF.
 */
class SplittableRemoteStageEvaluatorFactory implements TransformEvaluatorFactory {
  public static final String URN = "beam:directrunner:transforms:splittable_remote_stage:v1";

  // A fictional transform that transforms from KWI<unique key, KV<element, restriction>>
  // to simply KV<element, restriction> taken by the SDF inside the ExecutableStage.
  public static final String FEED_SDF_URN = "beam:directrunner:transforms:feed_sdf:v1";

  private final BundleFactory bundleFactory;
  private final JobBundleFactory jobBundleFactory;
  private final StepStateAndTimers.Provider stp;

  SplittableRemoteStageEvaluatorFactory(
      BundleFactory bundleFactory,
      JobBundleFactory jobBundleFactory,
      StepStateAndTimers.Provider stepStateAndTimers) {
    this.bundleFactory = bundleFactory;
    this.jobBundleFactory = jobBundleFactory;
    this.stp = stepStateAndTimers;
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) throws Exception {
    return new SplittableRemoteStageEvaluator(
        bundleFactory,
        jobBundleFactory,
        stp.forStepAndKey(application, inputBundle.getKey()),
        application);
  }

  @Override
  public void cleanup() throws Exception {
    jobBundleFactory.close();
  }

  private static class SplittableRemoteStageEvaluator<InputT, RestrictionT>
      implements TransformEvaluator<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> {
    private final PTransformNode transform;
    private final ExecutableStage stage;

    private final CopyOnAccessInMemoryStateInternals<byte[]> stateInternals;
    private final DirectTimerInternals timerInternals;
    private final RemoteBundle bundle;
    private final FnDataReceiver<WindowedValue<?>> mainInput;
    private final Collection<UncommittedBundle<?>> outputs;

    private final SDFFeederViaStateAndTimers<InputT, RestrictionT> feeder;

    private SplittableRemoteStageEvaluator(
        BundleFactory bundleFactory,
        JobBundleFactory jobBundleFactory,
        StepStateAndTimers<byte[]> stp,
        PTransformNode transform)
        throws Exception {
      this.stateInternals = stp.stateInternals();
      this.timerInternals = stp.timerInternals();
      this.transform = transform;
      this.stage =
          ExecutableStage.fromPayload(
              ExecutableStagePayload.parseFrom(transform.getTransform().getSpec().getPayload()));
      this.outputs = new ArrayList<>();

      FullWindowedValueCoder<KV<InputT, RestrictionT>> windowedValueCoder =
          (FullWindowedValueCoder<KV<InputT, RestrictionT>>)
              WireCoders.<KV<InputT, RestrictionT>>instantiateRunnerWireCoder(
                  stage.getInputPCollection(), stage.getComponents());
      KvCoder<InputT, RestrictionT> kvCoder =
          (KvCoder<InputT, RestrictionT>) windowedValueCoder.getValueCoder();
      this.feeder =
          new SDFFeederViaStateAndTimers<>(
              stateInternals,
              timerInternals,
              kvCoder.getKeyCoder(),
              kvCoder.getValueCoder(),
              (Coder<BoundedWindow>) windowedValueCoder.getWindowCoder());

      this.bundle =
          jobBundleFactory
              .forStage(stage)
              .getBundle(
                  BundleFactoryOutputReceiverFactory.create(
                      bundleFactory, stage.getComponents(), outputs::add),
                  StateRequestHandler.unsupported(),
                  // TODO: Wire in splitting via a split listener
                  new BundleProgressHandler() {
                    @Override
                    public void onProgress(ProcessBundleProgressResponse progress) {}

                    @Override
                    public void onCompleted(ProcessBundleResponse response) {}
                  });
      this.mainInput = Iterables.getOnlyElement(bundle.getInputReceivers().values());
    }

    @Override
    public void processElement(
        WindowedValue<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> windowedWorkItem)
        throws Exception {
      KeyedWorkItem<byte[], KV<InputT, RestrictionT>> kwi = windowedWorkItem.getValue();
      WindowedValue<KV<InputT, RestrictionT>> elementRestriction =
          Iterables.getOnlyElement(kwi.elementsIterable(), null);
      if (elementRestriction != null) {
        feeder.seed(elementRestriction);
      } else {
        elementRestriction = feeder.resume(Iterables.getOnlyElement(kwi.timersIterable()));
      }
      mainInput.accept(elementRestriction);
    }

    @Override
    public TransformResult<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> finishBundle()
        throws Exception {
      bundle.close();
      feeder.commit();
      CopyOnAccessInMemoryStateInternals<byte[]> state = stateInternals.commit();
      StepTransformResult.Builder<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> result =
          StepTransformResult.withHold(transform, state.getEarliestWatermarkHold());
      return result
          .addOutput(outputs)
          .withState(state)
          .withTimerUpdate(timerInternals.getTimerUpdate())
          .build();
    }
  }
}
