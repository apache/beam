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
package org.apache.beam.runners.fnexecution.control;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.ModelCoderRegistrar;
import org.apache.beam.sdk.util.construction.ModelCoders;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.FusedPipeline;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.ProtoOverrides;
import org.apache.beam.sdk.util.construction.graph.SplittableParDoExpander;
import org.apache.beam.sdk.util.construction.graph.TimerReference;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/** Tests for {@link ProcessBundleDescriptors}. */
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
public class ProcessBundleDescriptorsTest implements Serializable {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  /**
   * Tests that a stateful stage will wrap the key coder of a stateful transform in a
   * LengthPrefixCoder.
   */
  @Test
  public void testLengthPrefixingOfKeyCoderInStatefulExecutableStage() throws Exception {
    // Add another stateful stage with a non-standard key coder
    Pipeline p = Pipeline.create();
    Coder<Void> keycoder = VoidCoder.of();
    assertThat(ModelCoderRegistrar.isKnownCoder(keycoder), is(false));
    p.apply("impulse", Impulse.create())
        .apply(
            "create",
            ParDo.of(
                new DoFn<byte[], KV<Void, String>>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {}
                }))
        .setCoder(KvCoder.of(keycoder, StringUtf8Coder.of()))
        .apply(
            "userState",
            ParDo.of(
                new DoFn<KV<Void, String>, KV<Void, String>>() {

                  @StateId("stateId")
                  private final StateSpec<BagState<String>> bufferState =
                      StateSpecs.bag(StringUtf8Coder.of());

                  @TimerId("timerId")
                  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                  @ProcessElement
                  public void processElement(
                      @Element KV<Void, String> element,
                      @StateId("stateId") BagState<String> state,
                      @TimerId("timerId") Timer timer,
                      OutputReceiver<KV<Void, String>> r) {}

                  @OnTimer("timerId")
                  public void onTimer() {}
                }))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(
            fused.getFusedStages(),
            (ExecutableStage stage) ->
                stage.getUserStates().stream()
                    .anyMatch(spec -> spec.localName().equals("stateId")));
    checkState(optionalStage.isPresent(), "Expected a stage with user state.");

    ExecutableStage stage = optionalStage.get();
    PipelineNode.PCollectionNode inputPCollection = stage.getInputPCollection();

    // Ensure original key coder is not a LengthPrefixCoder
    Map<String, RunnerApi.Coder> stageCoderMap = stage.getComponents().getCodersMap();
    RunnerApi.Coder originalMainInputCoder =
        stageCoderMap.get(inputPCollection.getPCollection().getCoderId());
    String originalKeyCoderId =
        ModelCoders.getKvCoderComponents(originalMainInputCoder).keyCoderId();
    RunnerApi.Coder originalKeyCoder = stageCoderMap.get(originalKeyCoderId);
    assertThat(originalKeyCoder.getSpec().getUrn(), is(CoderTranslation.JAVA_SERIALIZED_CODER_URN));

    // Now create ProcessBundleDescriptor and check for the LengthPrefixCoder around the key coder
    BeamFnApi.ProcessBundleDescriptor pbd =
        ProcessBundleDescriptors.fromExecutableStage(
                "test_stage", stage, Endpoints.ApiServiceDescriptor.getDefaultInstance())
            .getProcessBundleDescriptor();
    Map<String, RunnerApi.Coder> pbsCoderMap = pbd.getCodersMap();

    RunnerApi.Coder pbsMainInputCoder =
        pbsCoderMap.get(pbd.getPcollectionsOrThrow(inputPCollection.getId()).getCoderId());
    String keyCoderId = ModelCoders.getKvCoderComponents(pbsMainInputCoder).keyCoderId();
    RunnerApi.Coder keyCoder = pbsCoderMap.get(keyCoderId);
    ensureLengthPrefixed(keyCoder, originalKeyCoder, pbsCoderMap);

    TimerReference timerRef = Iterables.getOnlyElement(stage.getTimers());
    String timerTransformId = timerRef.transform().getId();
    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.parseFrom(
            pbd.getTransformsOrThrow(timerTransformId).getSpec().getPayload());
    RunnerApi.TimerFamilySpec timerSpec =
        parDoPayload.getTimerFamilySpecsOrThrow(timerRef.localName());
    RunnerApi.Coder timerCoder = pbsCoderMap.get(timerSpec.getTimerFamilyCoderId());
    String timerKeyCoderId = timerCoder.getComponentCoderIds(0);
    RunnerApi.Coder timerKeyCoder = pbsCoderMap.get(timerKeyCoderId);
    ensureLengthPrefixed(timerKeyCoder, originalKeyCoder, pbsCoderMap);
  }

  @Test
  public void testLengthPrefixingOfInputCoderExecutableStage() throws Exception {
    Pipeline p = Pipeline.create();
    Coder<Void> voidCoder = VoidCoder.of();
    assertThat(ModelCoderRegistrar.isKnownCoder(voidCoder), is(false));
    p.apply("impulse", Impulse.create())
        .apply(
            ParDo.of(
                new DoFn<byte[], Void>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {}
                }))
        .setCoder(voidCoder)
        .apply(
            ParDo.of(
                new DoFn<Void, Void>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext context, RestrictionTracker<Void, Void> tracker) {}

                  @GetInitialRestriction
                  public Void getInitialRestriction() {
                    return null;
                  }

                  @NewTracker
                  public SomeTracker newTracker(@Restriction Void restriction) {
                    return null;
                  }
                }))
        .setCoder(voidCoder);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    RunnerApi.Pipeline pipelineWithSdfExpanded =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            pipelineProto,
            SplittableParDoExpander.createSizedReplacement());
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineWithSdfExpanded);
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(
            fused.getFusedStages(),
            (ExecutableStage stage) ->
                stage.getTransforms().stream()
                    .anyMatch(
                        transform ->
                            transform
                                .getTransform()
                                .getSpec()
                                .getUrn()
                                .equals(
                                    PTransformTranslation
                                        .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)));
    checkState(
        optionalStage.isPresent(),
        "Expected a stage with SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN.");

    ExecutableStage stage = optionalStage.get();
    PipelineNode.PCollectionNode inputPCollection = stage.getInputPCollection();
    Map<String, RunnerApi.Coder> stageCoderMap = stage.getComponents().getCodersMap();
    RunnerApi.Coder originalMainInputCoder =
        stageCoderMap.get(inputPCollection.getPCollection().getCoderId());

    BeamFnApi.ProcessBundleDescriptor pbd =
        ProcessBundleDescriptors.fromExecutableStage(
                "test_stage", stage, Endpoints.ApiServiceDescriptor.getDefaultInstance())
            .getProcessBundleDescriptor();
    Map<String, RunnerApi.Coder> pbsCoderMap = pbd.getCodersMap();

    RunnerApi.Coder pbsMainInputCoder =
        pbsCoderMap.get(pbd.getPcollectionsOrThrow(inputPCollection.getId()).getCoderId());

    RunnerApi.Coder kvCoder =
        pbsCoderMap.get(ModelCoders.getKvCoderComponents(pbsMainInputCoder).keyCoderId());
    RunnerApi.Coder keyCoder =
        pbsCoderMap.get(ModelCoders.getKvCoderComponents(kvCoder).keyCoderId());
    RunnerApi.Coder valueKvCoder =
        pbsCoderMap.get(ModelCoders.getKvCoderComponents(kvCoder).valueCoderId());
    RunnerApi.Coder valueCoder =
        pbsCoderMap.get(ModelCoders.getKvCoderComponents(valueKvCoder).keyCoderId());

    RunnerApi.Coder originalKvCoder =
        stageCoderMap.get(ModelCoders.getKvCoderComponents(originalMainInputCoder).keyCoderId());
    RunnerApi.Coder originalKeyCoder =
        stageCoderMap.get(ModelCoders.getKvCoderComponents(originalKvCoder).keyCoderId());
    RunnerApi.Coder originalvalueKvCoder =
        stageCoderMap.get(ModelCoders.getKvCoderComponents(originalKvCoder).valueCoderId());
    RunnerApi.Coder originalvalueCoder =
        stageCoderMap.get(ModelCoders.getKvCoderComponents(originalvalueKvCoder).keyCoderId());

    ensureLengthPrefixed(keyCoder, originalKeyCoder, pbsCoderMap);
    ensureLengthPrefixed(valueCoder, originalvalueCoder, pbsCoderMap);
  }

  private static void ensureLengthPrefixed(
      RunnerApi.Coder coder,
      RunnerApi.Coder originalCoder,
      Map<String, RunnerApi.Coder> pbsCoderMap) {
    assertThat(coder.getSpec().getUrn(), is(ModelCoders.LENGTH_PREFIX_CODER_URN));
    // Check that the wrapped coder is unchanged
    String lengthPrefixedWrappedCoderId = coder.getComponentCoderIds(0);
    assertThat(pbsCoderMap.get(lengthPrefixedWrappedCoderId), is(originalCoder));
  }

  private abstract static class SomeTracker extends RestrictionTracker<Void, Void> {}
}
