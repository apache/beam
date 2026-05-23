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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.junit.Test;

/**
 * Tests for {@link KafkaStreamsPipelineTranslator}.
 *
 * <p>The skeleton translator does not yet handle any transforms; these tests pin the current
 * "fail-fast with a clear URN" contract so that follow-up sub-issues can replace the assertions as
 * real translators are added.
 */
public class KafkaStreamsPipelineTranslatorTest {

  private static final String JOB_ID = "kafka-streams-test-job";

  @Test
  public void translateRejectsUnknownTransformWithUrnInMessage() {
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsTranslationContext context = newContext();

    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putTransforms(
                        "impulse",
                        RunnerApi.PTransform.newBuilder()
                            .setUniqueName("Impulse")
                            .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                    .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN))
                            .build()))
            .build();

    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> translator.translate(context, translator.prepareForTranslation(pipeline)));

    assertThat(ex.getMessage(), containsString("No translator registered for URN"));
    assertThat(ex.getMessage(), containsString(PTransformTranslation.IMPULSE_TRANSFORM_URN));
    assertThat(ex.getMessage(), containsString(JOB_ID));
  }

  @Test
  public void translateRejectsEmptyPipeline() {
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsTranslationContext context = newContext();

    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(RunnerApi.Components.newBuilder().build())
            .build();

    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> translator.translate(context, translator.prepareForTranslation(pipeline)));

    assertThat(ex.getMessage(), containsString("No translator registered"));
  }

  @Test
  public void createTranslationContextExposesJobInfoAndOptions() {
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    JobInfo jobInfo =
        JobInfo.create(
            JOB_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));

    KafkaStreamsTranslationContext context = translator.createTranslationContext(jobInfo, options);

    assertThat(context.getJobInfo().jobId(), containsString(JOB_ID));
    assertThat(context.getPipelineOptions().getBootstrapServers(), containsString("localhost"));
  }

  private static KafkaStreamsTranslationContext newContext() {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    JobInfo jobInfo =
        JobInfo.create(
            JOB_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));
    return KafkaStreamsTranslationContext.create(jobInfo, options);
  }
}
