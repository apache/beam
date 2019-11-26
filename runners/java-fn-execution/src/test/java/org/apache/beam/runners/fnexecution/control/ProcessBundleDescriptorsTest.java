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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.ModelCoderRegistrar;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.FusedPipeline;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;

/** Tests for {@link ProcessBundleDescriptors}. */
public class ProcessBundleDescriptorsTest implements Serializable {

  /**
   * Tests that a stateful stage will wrap the key coder of a stateful transform in a
   * LengthPrefixCoder.
   */
  @Test
  public void testWrapKeyCoderOfStatefulExecutableStageInLengthPrefixCoder() throws Exception {
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

                  @ProcessElement
                  public void processElement(
                      @Element KV<Void, String> element,
                      @StateId("stateId") BagState<String> state,
                      OutputReceiver<KV<Void, String>> r) {
                    for (String value : state.read()) {
                      r.output(KV.of(element.getKey(), value));
                    }
                    state.add(element.getValue());
                  }
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
    RunnerApi.Coder originalCoder =
        stageCoderMap.get(inputPCollection.getPCollection().getCoderId());
    String originalKeyCoderId = ModelCoders.getKvCoderComponents(originalCoder).keyCoderId();
    assertThat(
        stageCoderMap.get(originalKeyCoderId).getSpec().getUrn(),
        is(CoderTranslation.JAVA_SERIALIZED_CODER_URN));

    // Now create ProcessBundleDescriptor and check for the LengthPrefixCoder around the key coder
    BeamFnApi.ProcessBundleDescriptor pbDescriptor =
        ProcessBundleDescriptors.fromExecutableStage(
                "test_stage", stage, Endpoints.ApiServiceDescriptor.getDefaultInstance())
            .getProcessBundleDescriptor();

    String inputPCollectionId = inputPCollection.getId();
    String inputCoderId = pbDescriptor.getPcollectionsMap().get(inputPCollectionId).getCoderId();

    Map<String, RunnerApi.Coder> pbCoderMap = pbDescriptor.getCodersMap();
    RunnerApi.Coder coder = pbCoderMap.get(inputCoderId);
    String keyCoderId = ModelCoders.getKvCoderComponents(coder).keyCoderId();

    RunnerApi.Coder keyCoder = pbCoderMap.get(keyCoderId);
    // Ensure length prefix
    assertThat(keyCoder.getSpec().getUrn(), is(ModelCoders.LENGTH_PREFIX_CODER_URN));
    String lengthPrefixWrappedCoderId = keyCoder.getComponentCoderIds(0);

    // Check that the wrapped coder is unchanged
    assertThat(lengthPrefixWrappedCoderId, is(originalKeyCoderId));
    assertThat(
        pbCoderMap.get(lengthPrefixWrappedCoderId), is(stageCoderMap.get(originalKeyCoderId)));
  }
}
