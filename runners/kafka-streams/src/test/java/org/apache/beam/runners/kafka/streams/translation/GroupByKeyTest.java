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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Test;

/**
 * End-to-end test of GroupByKey: {@code Impulse -> emit KVs -> GroupByKey -> record groups}.
 *
 * <p>GroupByKey shuffles through an internal repartition topic. {@link TopologyTestDriver} does not
 * loop a low-level sink topic back into its source, so the test drives the upstream, drains the
 * repartition topic, and pipes those records back into it — standing in for the broker round-trip.
 * The downstream {@code RecordGroupFn} records each emitted group into a {@link
 * SharedTestCollector}.
 */
public class GroupByKeyTest {

  private static final String JOB_ID = "ks-gbk-test";
  private static final String APPLICATION_ID = "ks-gbk-test";

  /** Emits a few KVs from the single impulse element so there is something to group. */
  private static class EmitKvsFn extends DoFn<byte[], KV<String, Integer>> {
    @ProcessElement
    public void processElement(OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("a", 1));
      out.output(KV.of("a", 2));
      out.output(KV.of("b", 3));
    }
  }

  /** Records each grouped result as {@code "key=[sorted values]"}. */
  private static class RecordGroupFn extends DoFn<KV<String, Iterable<Integer>>, Void> {
    private final SharedTestCollector<String> collector;

    RecordGroupFn(SharedTestCollector<String> collector) {
      this.collector = collector;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Integer>> group) {
      List<Integer> values = new ArrayList<>();
      group.getValue().forEach(values::add);
      Collections.sort(values);
      collector.record(group.getKey() + "=" + values);
    }
  }

  @Test
  public void groupsValuesByKeyAndFiresAtWatermark() throws Exception {
    try (SharedTestCollector<String> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(pipelineOptions());
      pipeline
          .apply("impulse", Impulse.create())
          .apply("emit", ParDo.of(new EmitKvsFn()))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
          .apply("gbk", GroupByKey.create())
          .apply("record", ParDo.of(new RecordGroupFn(collector)));

      RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
      KafkaStreamsPipelineOptions options =
          pipeline.getOptions().as(KafkaStreamsPipelineOptions.class);
      KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
      JobInfo jobInfo =
          JobInfo.create(
              JOB_ID, options.getJobName(), "", PipelineOptionsTranslation.toProto(options));
      KafkaStreamsTranslationContext context =
          translator.createTranslationContext(jobInfo, options);

      RunnerApi.Pipeline prepared = translator.prepareForTranslation(pipelineProto);
      translator.translate(context, prepared);
      String repartitionTopic = GroupByKeyTranslator.repartitionTopic(findGroupByKeyId(prepared));

      Topology topology = context.getTopology();
      try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig())) {
        // Fire the impulse; the upstream stage emits the KVs and the terminal watermark, which the
        // re-key processor sends to the repartition sink.
        driver.advanceWallClockTime(Duration.ofSeconds(1));
        driver.advanceWallClockTime(Duration.ofSeconds(1));

        // Round-trip the repartition topic: drain what the sink wrote and feed it back to the
        // source so the GroupByKey processor buffers the values and fires at the watermark.
        TestOutputTopic<byte[], byte[]> repartitionOut =
            driver.createOutputTopic(
                repartitionTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        TestInputTopic<byte[], byte[]> repartitionIn =
            driver.createInputTopic(
                repartitionTopic, new ByteArraySerializer(), new ByteArraySerializer());
        for (TestRecord<byte[], byte[]> record : repartitionOut.readRecordsToList()) {
          repartitionIn.pipeInput(record);
        }
      }

      List<String> groups = collector.recorded();
      assertThat(groups.size(), is(2));
      assertThat(groups, hasItems("a=[1, 2]", "b=[3]"));
    }
  }

  private static String findGroupByKeyId(RunnerApi.Pipeline pipeline) {
    return pipeline.getComponents().getTransformsMap().entrySet().stream()
        .filter(
            e ->
                PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN.equals(
                    e.getValue().getSpec().getUrn()))
        .map(java.util.Map.Entry::getKey)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no GroupByKey transform in the pipeline"));
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
