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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Test;

/** Tests for {@link KafkaStreamsPipelineTranslator}. */
public class KafkaStreamsPipelineTranslatorTest {

  private static final String JOB_ID = "kafka-streams-test-job";
  private static final String OUTPUT_PCOLLECTION_ID = "impulse.out";

  @Test
  public void translateRejectsUnknownTransformWithUrnInMessage() {
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsTranslationContext context = newContext();

    // A URN the runner does not register a translator for.
    String unsupportedUrn = "beam:transform:kafka_streams_unsupported_test:v1";
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .addRootTransformIds("unsupported")
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putTransforms(
                        "unsupported",
                        RunnerApi.PTransform.newBuilder()
                            .setUniqueName("Unsupported")
                            .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(unsupportedUrn))
                            .build()))
            .build();

    // translate() directly — this test pins the URN-rejection contract on the dispatch loop
    // itself, independent of the fuser/validator that prepareForTranslation runs.
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class, () -> translator.translate(context, pipeline));

    assertThat(ex.getMessage(), containsString("No translator registered for URN"));
    assertThat(ex.getMessage(), containsString(unsupportedUrn));
    assertThat(ex.getMessage(), containsString("unsupported"));
    assertThat(ex.getMessage(), containsString(JOB_ID));
  }

  @Test
  public void translateImpulsePipelineAddsSourceAndProcessorNodes() {
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsTranslationContext context = newContext();

    // Build the pipeline through the SDK so the resulting RunnerApi.Pipeline carries the coders
    // and windowing strategies that PipelineValidator requires (run inside the fuser).
    Pipeline sdkPipeline =
        Pipeline.create(
            PipelineOptionsFactory.fromArgs(
                    "--applicationId=ks-translator-test",
                    "--runner=" + CrashingRunner.class.getName())
                .create());
    sdkPipeline.apply("impulse", Impulse.create());
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(sdkPipeline);

    translator.translate(context, translator.prepareForTranslation(pipeline));

    TopologyDescription description = context.getTopology().describe();
    String describeText = description.toString();
    assertThat(describeText, containsString("Source:"));
    assertThat(describeText, containsString("Processor:"));
  }

  @Test
  public void createTranslationContextExposesJobInfoAndOptions() {
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsPipelineOptions options = testOptions();
    JobInfo jobInfo =
        JobInfo.create(
            JOB_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));

    KafkaStreamsTranslationContext context = translator.createTranslationContext(jobInfo, options);

    assertThat(context.getJobInfo().jobId(), containsString(JOB_ID));
    assertThat(context.getPipelineOptions().getBootstrapServers(), containsString("localhost"));
  }

  static RunnerApi.Pipeline singleImpulsePipeline() {
    return RunnerApi.Pipeline.newBuilder()
        .addRootTransformIds("impulse")
        .setComponents(
            RunnerApi.Components.newBuilder()
                .putTransforms(
                    "impulse",
                    RunnerApi.PTransform.newBuilder()
                        .setUniqueName("Impulse")
                        .putOutputs("out", OUTPUT_PCOLLECTION_ID)
                        .setSpec(
                            RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                        .build())
                .putPcollections(
                    OUTPUT_PCOLLECTION_ID,
                    RunnerApi.PCollection.newBuilder().setUniqueName(OUTPUT_PCOLLECTION_ID).build())
                .build())
        .build();
  }

  static KafkaStreamsTranslationContext newContext() {
    KafkaStreamsPipelineOptions options = testOptions();
    JobInfo jobInfo =
        JobInfo.create(
            JOB_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));
    return KafkaStreamsTranslationContext.create(jobInfo, options);
  }

  static KafkaStreamsPipelineOptions testOptions() {
    return PipelineOptionsFactory.fromArgs("--applicationId=ks-translator-test")
        .as(KafkaStreamsPipelineOptions.class);
  }
}
