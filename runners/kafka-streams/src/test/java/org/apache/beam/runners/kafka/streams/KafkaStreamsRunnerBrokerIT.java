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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import org.testcontainers.kafka.KafkaContainer;
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
    // The official Apache Kafka image. 4.0.0 rather than the 3.9.0 the runner's client is built
    // against: Testcontainers' KafkaContainer cannot bring up the 3.9.0 image (it exits during
    // startup), and a client talking to a newer broker is the compatibility direction Kafka
    // supports anyway.
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));
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
    return options(topicPartitions, "ks-broker-it-" + UUID.randomUUID());
  }

  /**
   * Options for one runner instance.
   *
   * <p>Two instances of the same job share an application id — that is what puts them in one
   * consumer group and so splits the work between them — but each needs its own state directory,
   * since the local stores are per instance.
   */
  private KafkaStreamsPipelineOptions options(int topicPartitions, String applicationId) {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    options.setBootstrapServers(kafka.getBootstrapServers());
    options.setApplicationId(applicationId);
    options.setInternalParallelism(topicPartitions);
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

  @Test
  public void aBoundedPipelineTerminatesOnItsOwn() throws Exception {
    // Kafka Streams runs a topology until something stops the client, so a bounded pipeline used to
    // run for ever against a real broker: it produced the right answer and then sat there. The
    // other tests here cannot see that, because they cancel rather than wait, and the
    // ValidatesRunner
    // suite cannot either, because TopologyTestDriver is synchronous and always reports DONE.
    //
    // Nothing cancels this one. Returning from run() at all is the assertion.
    KafkaStreamsPipelineOptions options = options(4);
    Pipeline pipeline = Pipeline.create(options);
    buildChainedPipeline(pipeline);

    PipelineResult result = runPipeline(pipeline, options);

    assertThat(result.getState(), is(PipelineResult.State.DONE));
    // And it stopped for the right reason — having produced its output exactly once. Termination is
    // driven from a wall-clock punctuator, which is the same mechanism that duplicates output when
    // it is used to close bundles on time (#39633), so the count matters as much as the state.
    assertThat(counterValue(result), is(1L));
  }

  @Test
  public void twoInstancesShareThreePartitions() throws Exception {
    // Everything else here runs one instance, which leaves the thing the runner exists for
    // untested: the work being split between instances by Kafka's own group membership.
    //
    // Three partitions across two instances is deliberate. It does not divide, so the instances
    // take an unequal share, and a watermark aggregator on either of them has to hear from all
    // three upstream partitions — some of which are being produced by the other instance — before
    // it may let its watermark advance. If the reports were tied to the instance that produced
    // them rather than to the partition, this is the shape that would break.
    String applicationId = "ks-broker-it-" + UUID.randomUUID();
    List<PipelineResult> results = Collections.synchronizedList(new ArrayList<>());
    ExecutorService instances = Executors.newFixedThreadPool(2);
    try {
      List<Future<?>> running = new ArrayList<>();
      for (int instance = 0; instance < 2; instance++) {
        KafkaStreamsPipelineOptions options = options(3, applicationId);
        Pipeline pipeline = Pipeline.create(options);
        buildChainedPipeline(pipeline);
        running.add(
            instances.submit(
                () -> {
                  // run() blocks until its instance has finished, so each needs its own thread.
                  results.add(runPipeline(pipeline, options));
                }));
      }
      for (Future<?> future : running) {
        // Fails rather than hangs if an instance never finishes — which is the interesting way for
        // this to go wrong, since an instance only stops once every processor it owns is done.
        future.get(TIMEOUT.getMillis(), TimeUnit.MILLISECONDS);
      }
    } finally {
      instances.shutdownNow();
    }

    // The pipeline collapses everything onto one key, so exactly one group comes out of the second
    // GroupByKey however the partitions were shared. Each instance counts what it processed, so the
    // total across both is what has to be one: a group counted twice would mean the instances had
    // both claimed the same partition's data.
    long groups = 0;
    for (PipelineResult result : results) {
      groups += counterValue(result);
    }
    assertThat(groups, is(1L));
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
