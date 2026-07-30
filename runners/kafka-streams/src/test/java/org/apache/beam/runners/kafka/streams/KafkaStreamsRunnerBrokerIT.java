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
package org.apache.beam.runners.kafka.streams;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Files;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs a pipeline through the production {@link KafkaStreamsPipelineRunner} against a real Kafka
 * broker, rather than through the {@code TopologyTestDriver} the rest of the suite uses.
 *
 * <p>The test driver stands in for a broker well enough for translation and windowing logic, but it
 * runs one instance in one thread and fakes the topics. Everything that only exists on a real
 * cluster is untested by it: the runner creating its own bootstrap and repartition topics, records
 * actually round-tripping through a repartition topic, exactly-once processing, the state stores'
 * changelog, and the Kafka Streams application lifecycle. This test covers that path.
 *
 * <p>It needs Docker and so is not part of the default build; the {@code brokerIntegrationTest}
 * Gradle task runs it.
 */
@RunWith(JUnit4.class)
public class KafkaStreamsRunnerBrokerIT {

  private static final String NAMESPACE = "brokerIT";
  private static final String GROUPS_COUNTER = "groups";

  /** How long to wait for the streaming application to work through the pipeline. */
  private static final Duration TIMEOUT = Duration.standardMinutes(2);

  private static KafkaContainer kafka;

  @BeforeClass
  public static void startBroker() {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    kafka.start();
  }

  @AfterClass
  public static void stopBroker() {
    if (kafka != null) {
      kafka.stop();
    }
  }

  /** Emits a fixed set of keyed elements, one per key group. */
  private static class EmitKvsFn extends DoFn<byte[], KV<String, Integer>> {
    @ProcessElement
    public void processElement(OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("a", 1));
      out.output(KV.of("a", 2));
      out.output(KV.of("b", 3));
    }
  }

  /** Counts the groups that come out of the GroupByKey. */
  private static class CountGroupsFn extends DoFn<KV<String, Iterable<Integer>>, Void> {
    private final Counter groups = Metrics.counter(NAMESPACE, GROUPS_COUNTER);

    @ProcessElement
    public void processElement() {
      groups.inc();
    }
  }

  private KafkaStreamsPipelineOptions options() {
    return options(1);
  }

  private KafkaStreamsPipelineOptions options(int topicPartitions) {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    options.setBootstrapServers(kafka.getBootstrapServers());
    options.setApplicationId("ks-broker-it-" + UUID.randomUUID());
    options.setTopicPartitions(topicPartitions);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    try {
      options.setStateDir(Files.createTempDirectory("ks-broker-it").toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return options;
  }

  @Test
  public void groupByKeyRunsThroughARealBrokerAndReportsMetrics() throws Exception {
    KafkaStreamsPipelineOptions options = options();
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Impulse.create())
        .apply("emit", ParDo.of(new EmitKvsFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(GroupByKey.create())
        .apply("countGroups", ParDo.of(new CountGroupsFn()));

    // The same conversion the test runner does: translate Read-based sources as the primitive Read
    // the runner supports rather than the splittable-DoFn expansion.
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
    JobInfo jobInfo =
        JobInfo.create(
            options.getApplicationId(),
            options.getJobName(),
            "",
            PipelineOptionsTranslation.toProto(options));

    PipelineResult result = new KafkaStreamsPipelineRunner(options).run(pipelineProto, jobInfo);
    try {
      // Two keys in, so two groups out once the elements have travelled through the repartition
      // topic and the watermark has closed the global window.
      assertThat(awaitCounter(result, 2L), is(2L));
    } finally {
      result.cancel();
    }
  }

  /**
   * Collapses every group onto one key, so the next GroupByKey has to shuffle across partitions.
   */
  private static class ToSingleKeyFn
      extends DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>> {
    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<Integer>> group, OutputReceiver<KV<String, Integer>> out) {
      int sum = 0;
      for (int value : group.getValue()) {
        sum += value;
      }
      out.output(KV.of("all", sum));
    }
  }

  /** Builds the two-GroupByKey pipeline used by the chained tests. */
  private static void buildChainedPipeline(Pipeline pipeline) {
    pipeline
        .apply(Impulse.create())
        .apply("emit", ParDo.of(new EmitKvsFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply("groupPerKey", GroupByKey.create())
        .apply("toSingleKey", ParDo.of(new ToSingleKeyFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply("groupAll", GroupByKey.create())
        .apply("countGroups", ParDo.of(new CountGroupsFn()));
  }

  private static PipelineResult runPipeline(
      Pipeline pipeline, KafkaStreamsPipelineOptions options) {
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
    JobInfo jobInfo =
        JobInfo.create(
            options.getApplicationId(),
            options.getJobName(),
            "",
            PipelineOptionsTranslation.toProto(options));
    return new KafkaStreamsPipelineRunner(options).run(pipelineProto, jobInfo);
  }

  @Test
  public void chainedGroupByKeysAreCorrectOnOnePartition() throws Exception {
    // The control for the partitioned case below: the same shape, one partition throughout.
    KafkaStreamsPipelineOptions options = options(1);
    Pipeline pipeline = Pipeline.create(options);
    buildChainedPipeline(pipeline);

    PipelineResult result = runPipeline(pipeline, options);
    try {
      assertThat(awaitCounter(result, 1L), is(1L));
    } finally {
      result.cancel();
    }
  }

  @Test
  public void chainedGroupByKeysAreCorrectAcrossPartitions() throws Exception {
    // Two GroupByKeys with a partitioned shuffle between them, which is what makes each task's
    // watermark identity matter. The second GroupByKey aggregates the reports of every task of the
    // first, so those tasks have to report under their own partition: if each claimed to be the
    // only partition, the second would advance its watermark on the first report it saw and fire
    // before the remaining partitions had contributed their groups.
    KafkaStreamsPipelineOptions options = options(4);
    Pipeline pipeline = Pipeline.create(options);
    buildChainedPipeline(pipeline);

    PipelineResult result = runPipeline(pipeline, options);
    try {
      // Everything collapses onto one key, so the second GroupByKey emits exactly one group — and
      // only once every partition of the first has contributed to it.
      assertThat(awaitCounter(result, 1L), is(1L));
      // A premature firing would show up as a second group, so give one a chance to appear.
      Thread.sleep(5_000L);
      assertThat(counterValue(result), is(1L));
    } finally {
      result.cancel();
    }
  }

  /**
   * Polls the pipeline's metrics until the counter reaches {@code expected} or the timeout hits.
   */
  private static long awaitCounter(PipelineResult result, long expected) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT.getMillis();
    long value = 0;
    while (System.currentTimeMillis() < deadline) {
      value = counterValue(result);
      if (value >= expected) {
        return value;
      }
      Thread.sleep(500L);
    }
    return value;
  }

  private static long counterValue(PipelineResult result) {
    MetricQueryResults query =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(NAMESPACE, GROUPS_COUNTER))
                    .build());
    if (Iterables.isEmpty(query.getCounters())) {
      return 0L;
    }
    MetricResult<Long> counter = Iterables.getOnlyElement(query.getCounters());
    return counter.getAttempted();
  }
}
