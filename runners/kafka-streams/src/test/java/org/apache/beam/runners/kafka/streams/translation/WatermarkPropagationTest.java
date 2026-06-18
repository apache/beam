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
import java.util.ArrayList;
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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Test;

/**
 * End-to-end test that a watermark propagates through the topology: {@code Impulse ->
 * ExecutableStage -> a recording sink}. The Impulse source emits a terminal {@code
 * TIMESTAMP_MAX_VALUE} watermark report; the ExecutableStage routes it through its {@link
 * WatermarkManager} and forwards it on, stamped as its own single source partition. A sink
 * processor attached to the leaf captures the forwarded watermark and the test asserts on it.
 */
public class WatermarkPropagationTest {

  private static final String APPLICATION_ID = "ks-watermark-propagation-test";

  /** Identity DoFn so the pipeline contains a fused ExecutableStage. */
  private static class IdentityFn extends DoFn<byte[], byte[]> {
    @ProcessElement
    public void processElement(@Element byte[] input, OutputReceiver<byte[]> out) {
      out.output(input);
    }
  }

  /** Sink processor that records the watermark payloads it is forwarded. */
  private static final class WatermarkCapture
      implements Processor<byte[], KStreamsPayload<?>, Void, Void> {
    private final List<KStreamsPayload<?>> watermarks;

    WatermarkCapture(List<KStreamsPayload<?>> watermarks) {
      this.watermarks = watermarks;
    }

    @Override
    public void process(Record<byte[], KStreamsPayload<?>> record) {
      if (record.value().isWatermark()) {
        watermarks.add(record.value());
      }
    }
  }

  @Test
  public void terminalWatermarkPropagatesToDownstreamStampedAsSingleSource() throws Exception {
    Pipeline pipeline = Pipeline.create(pipelineOptions());
    pipeline.apply("impulse", Impulse.create()).apply("identity", ParDo.of(new IdentityFn()));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
    KafkaStreamsPipelineOptions options =
        pipeline.getOptions().as(KafkaStreamsPipelineOptions.class);
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    JobInfo jobInfo =
        JobInfo.create(
            APPLICATION_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));
    KafkaStreamsTranslationContext context = translator.createTranslationContext(jobInfo, options);
    translator.translate(context, translator.prepareForTranslation(pipelineProto));

    // Attach a sink to the leaf ExecutableStage processor to capture the watermark it forwards.
    Topology topology = context.getTopology();
    List<KStreamsPayload<?>> captured = new ArrayList<>();
    topology.addProcessor(
        "watermark-capture", () -> new WatermarkCapture(captured), findLeafProcessor(topology));

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig())) {
      driver.advanceWallClockTime(Duration.ofSeconds(1));
      driver.advanceWallClockTime(Duration.ofSeconds(1));
    }

    assertThat("a watermark reached the downstream sink", captured.isEmpty(), is(false));
    WatermarkPayload terminal = captured.get(captured.size() - 1).asWatermark();
    assertThat(terminal.getWatermarkMillis(), is(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    assertThat(terminal.getSourcePartition(), is(0));
    assertThat(terminal.getTotalSourcePartitions(), is(1));
  }

  /**
   * Returns the name of the single processor node with no successors (the leaf of the topology).
   */
  private static String findLeafProcessor(Topology topology) {
    for (TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
      for (TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof TopologyDescription.Processor && node.successors().isEmpty()) {
          return node.name();
        }
      }
    }
    throw new IllegalStateException("no leaf processor found in topology");
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
