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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Measures how long work takes to move to another instance when the instance doing it goes away.
 *
 * <p>This is the claim the runner is proposed on: because Kafka Streams divides work by partition
 * and keeps its state in Kafka, losing an instance is a consumer group rebalance rather than a
 * restart of the job from a checkpoint.
 *
 * <p>Each instance stamps what it processes with its own name, which is what makes the measurement
 * mean anything. The runner reads an unbounded source with a single reader (<a
 * href="https://github.com/apache/beam/issues/39626">#39626</a>), so at any moment exactly one
 * instance is doing the work — removing the other would prove nothing and would look like a fast
 * handover. This finds the instance that is actually producing, removes that one, and times how
 * long until the other produces.
 *
 * <p><b>What this does not measure.</b> An instance removed here shuts down in an orderly way, and
 * this pipeline holds no state. A machine that dies does neither: its absence cannot be noticed
 * until the group's {@code session.timeout.ms} expires, 45 seconds by default, and a pipeline with
 * state has to restore it. So this is the cost of moving stateless work, and a lower bound on
 * anything else.
 */
@RunWith(JUnit4.class)
public class KafkaStreamsRunnerRescalingIT {

  /** How many elements each instance has processed, by the name that instance was given. */
  private static final Map<String, AtomicLong> PROCESSED = new ConcurrentHashMap<>();

  private static final String FIRST = "instance-1";
  private static final String SECOND = "instance-2";
  private static final int PARTITIONS = 3;

  /** Enough elements that an instance is clearly the one doing the work, not just starting up. */
  private static final long CLEARLY_PRODUCING = 1000L;

  private static KafkaContainer kafka;

  @BeforeClass
  public static void startBroker() {
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));
    kafka.start();
  }

  @AfterClass
  public static void stopBroker() {
    if (kafka != null) {
      kafka.stop();
    }
  }

  @Before
  public void clearCounts() {
    PROCESSED.clear();
  }

  private static long processedBy(String instance) {
    AtomicLong count = PROCESSED.get(instance);
    return count == null ? 0L : count.get();
  }

  /** Counts what reached the end of the pipeline, per instance. */
  private static class RecordArrivalFn extends DoFn<Long, Void> {
    // Serialized with the pipeline, so each instance's copy knows which instance it belongs to.
    private final String instance;

    RecordArrivalFn(String instance) {
      this.instance = instance;
    }

    @ProcessElement
    public void processElement() {
      PROCESSED.computeIfAbsent(instance, key -> new AtomicLong()).incrementAndGet();
    }
  }

  private KafkaStreamsPipelineOptions options(String applicationId) {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    options.setBootstrapServers(kafka.getBootstrapServers());
    options.setApplicationId(applicationId);
    options.setInternalParallelism(PARTITIONS);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    try {
      options.setStateDir(Files.createTempDirectory("ks-rescaling-it").toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return options;
  }

  private Future<?> startInstance(ExecutorService executor, String applicationId, String instance) {
    KafkaStreamsPipelineOptions options = options(applicationId);
    Pipeline pipeline = Pipeline.create(options);
    // No GroupByKey: a windowed group emits only when a window fires, and the bundle downstream of
    // it closes only on element count or on a watermark arriving (#39633), so its output comes in
    // bursts tens of seconds apart — far coarser than what is being measured here. Reading and
    // counting gives a steady signal, and the task reading the source is what has to move.
    pipeline
        .apply("read", Read.from(CountingSource.unbounded()))
        .apply("record", ParDo.of(new RecordArrivalFn(instance)));
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(pipeline);
    JobInfo jobInfo =
        JobInfo.create(
            options.getApplicationId(),
            options.getJobName(),
            "",
            PipelineOptionsTranslation.toProto(options));
    return executor.submit(
        () -> {
          // Blocks for as long as the instance runs; cancelling the future interrupts it, which the
          // runner treats as a request to stop.
          new KafkaStreamsPipelineRunner(options).run(proto, jobInfo);
        });
  }

  /** Returns the instance doing the work, once exactly one of them clearly is. */
  private static String awaitProducingInstance(Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.getMillis();
    while (System.currentTimeMillis() < deadline) {
      long first = processedBy(FIRST);
      long second = processedBy(SECOND);
      if (first >= CLEARLY_PRODUCING && second == 0) {
        return FIRST;
      }
      if (second >= CLEARLY_PRODUCING && first == 0) {
        return SECOND;
      }
      if (first >= CLEARLY_PRODUCING && second >= CLEARLY_PRODUCING) {
        throw new AssertionError(
            "Both instances are processing, so removing one does not isolate a handover: "
                + FIRST
                + "="
                + first
                + ", "
                + SECOND
                + "="
                + second);
      }
      Thread.sleep(200L);
    }
    throw new AssertionError(
        "Neither instance started processing within " + timeout + ": " + PROCESSED);
  }

  @Test
  public void workMovesToTheOtherInstanceWhenOneIsRemoved() throws Exception {
    String applicationId = "ks-rescaling-it-" + UUID.randomUUID();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Map<String, Future<?>> futures = new HashMap<>();
      futures.put(FIRST, startInstance(executor, applicationId, FIRST));
      futures.put(SECOND, startInstance(executor, applicationId, SECOND));

      String producing = awaitProducingInstance(Duration.standardMinutes(2));
      String idle = producing.equals(FIRST) ? SECOND : FIRST;

      long removedAt = System.currentTimeMillis();
      futures.get(producing).cancel(true);

      // The handover is the idle instance producing anything at all: it had processed nothing.
      long deadline = removedAt + Duration.standardMinutes(3).getMillis();
      long tookOverAt = -1L;
      while (System.currentTimeMillis() < deadline) {
        if (processedBy(idle) > 0) {
          tookOverAt = System.currentTimeMillis();
          break;
        }
        Thread.sleep(50L);
      }

      long handover = tookOverAt < 0 ? -1L : tookOverAt - removedAt;
      System.out.println(
          "HANDOVER: removed "
              + producing
              + " after it processed "
              + processedBy(producing)
              + " elements; "
              + idle
              + " took over after "
              + handover
              + " ms");

      assertThat(
          "the remaining instance never took the work over", handover, greaterThanOrEqualTo(0L));
      // Kafka Streams does not leave the group when it closes, so the handover may have to wait for
      // the group to notice the instance has gone. The bound is well above the default session
      // timeout, so that the number gets reported rather than the test just failing.
      assertThat(handover, lessThan(Duration.standardMinutes(2).getMillis()));
      futures.get(idle).cancel(true);
    } finally {
      executor.shutdownNow();
    }
  }
}
