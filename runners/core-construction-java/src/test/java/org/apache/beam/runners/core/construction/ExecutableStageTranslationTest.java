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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;

/** Tests for {@link ExecutableStageTranslation}. */
public class ExecutableStageTranslationTest implements Serializable {

  @Test
  /* Test for generating readable operator names during translation. */
  public void testOperatorNameGeneration() throws Exception {
    Pipeline p = Pipeline.create();
    p.apply(Impulse.create())
        // Anonymous ParDo
        .apply(
            ParDo.of(
                new DoFn<byte[], String>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<String> outputReceiver) {}
                }))
        // Name ParDo
        .apply(
            "MyName",
            ParDo.of(
                new DoFn<String, Integer>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<Integer> outputReceiver) {}
                }))
        .apply(
            // This is how Python pipelines construct names
            "ref_AppliedPTransform_count",
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<Integer> outputReceiver) {}
                }));

    ExecutableStage firstEnvStage =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p))
            .getFusedStages()
            .stream()
            .findFirst()
            .get();
    RunnerApi.ExecutableStagePayload basePayload =
        RunnerApi.ExecutableStagePayload.parseFrom(
            firstEnvStage.toPTransform("foo").getSpec().getPayload());

    String executableStageName =
        ExecutableStageTranslation.generateNameFromStagePayload(basePayload);

    assertThat(executableStageName, is("[3]{ParDo(Anonymous), MyName, count}"));
  }
}
