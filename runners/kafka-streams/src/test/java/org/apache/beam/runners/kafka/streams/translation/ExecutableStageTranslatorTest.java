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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;

/**
 * End-to-end test for {@link ExecutableStageTranslator}: builds an {@code Impulse -> ParDo}
 * pipeline with the high-level Beam Java SDK, fuses + translates it, and runs the resulting Kafka
 * Streams topology under {@link TopologyTestDriver}. The fused ParDo executes in an in-process
 * (EMBEDDED) Java SDK harness, so the {@link DoFn}'s {@code @ProcessElement} body runs for real —
 * no Docker, no broker.
 *
 * <p>Because the ParDo's output PCollection has no downstream consumer, it is not a stage output
 * and is never forwarded out of the harness — that is the documented behaviour. The test verifies
 * the bridge works by having the DoFn record into a {@link SharedTestCollector} as a side effect
 * and asserting the recorded input from the test thread.
 */
public class ExecutableStageTranslatorTest {

  private static final String JOB_ID = "kafka-streams-executable-stage-test";
  private static final String APPLICATION_ID = "ks-executable-stage-test";

  /**
   * Records the length of every input element seen by the harness so the test can verify the DoFn
   * ran. {@link SharedTestCollector} carries its identity via a UUID stored on the instance itself,
   * so it survives any serialization the runner may perform on the DoFn.
   */
  private static class RecordingFn extends DoFn<byte[], byte[]> {
    private final SharedTestCollector<Integer> collector;

    RecordingFn(SharedTestCollector<Integer> collector) {
      this.collector = collector;
    }

    @ProcessElement
    public void processElement(@Element byte[] input, OutputReceiver<byte[]> out) {
      collector.record(input.length);
      // Still emit something so the output codepath of the harness is exercised, even though no
      // downstream consumer means the runner never observes the value.
      out.output(new byte[] {1});
    }
  }

  @Test
  public void impulseThenParDoExecutesDoFnInHarnessOncePerImpulseElement() throws Exception {
    try (SharedTestCollector<Integer> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(pipelineOptions());
      pipeline
          .apply("impulse", Impulse.create())
          .apply("pardo", ParDo.of(new RecordingFn(collector)));

      RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

      KafkaStreamsPipelineOptions options =
          pipeline.getOptions().as(KafkaStreamsPipelineOptions.class);
      KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
      JobInfo jobInfo =
          JobInfo.create(
              JOB_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));
      KafkaStreamsTranslationContext context =
          translator.createTranslationContext(jobInfo, options);

      translator.translate(context, translator.prepareForTranslation(pipelineProto));

      Topology topology = context.getTopology();
      try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig())) {
        driver.advanceWallClockTime(Duration.ofSeconds(1));
        driver.advanceWallClockTime(Duration.ofSeconds(1));
      }

      List<Integer> recorded = collector.recorded();
      // Impulse emits exactly one empty byte[] in the GlobalWindow, so the DoFn must run exactly
      // once and see a zero-length input.
      assertThat(recorded.size(), is(1));
      assertThat(recorded.get(0), is(0));
    }
  }

  private static PipelineOptions pipelineOptions() {
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs("--applicationId=" + APPLICATION_ID).create();
    options.setRunner(CrashingRunner.class);
    options.as(KafkaStreamsPipelineOptions.class).setApplicationId(APPLICATION_ID);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    return options;
  }

  private static Properties streamsConfig() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    props.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    return props;
  }
}
