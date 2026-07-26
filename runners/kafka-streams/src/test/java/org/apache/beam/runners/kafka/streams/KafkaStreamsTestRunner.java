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

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.translation.KafkaStreamsPipelineTranslator;
import org.apache.beam.runners.kafka.streams.translation.KafkaStreamsTranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Test harness that runs a Beam {@link Pipeline} through the Kafka Streams runner's translation and
 * a {@link TopologyTestDriver}, so tests do not repeat the translate + drive boilerplate.
 *
 * <p>Usage: build a pipeline with {@link #testOptions()}, then call {@link #run(Pipeline)}. Side
 * effects (e.g. a {@code SharedTestCollector} written by a recording DoFn) have completed when it
 * returns.
 *
 * <p>{@link TopologyTestDriver} loops each internal repartition topic (one that is both a sink and
 * a source in the topology — e.g. the one GroupByKey introduces) from its sink back to its source
 * within a single driver step, so advancing the wall clock is enough to drive the whole pipeline to
 * completion; no manual broker simulation is needed.
 */
public final class KafkaStreamsTestRunner {

  private KafkaStreamsTestRunner() {}

  /** Pipeline options for a Kafka Streams runner test: the EMBEDDED harness and a unique app id. */
  public static PipelineOptions testOptions() {
    String applicationId = "ks-test-" + UUID.randomUUID();
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs("--applicationId=" + applicationId).create();
    options.setRunner(CrashingRunner.class);
    options.as(KafkaStreamsPipelineOptions.class).setApplicationId(applicationId);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    return options;
  }

  /**
   * Translates the pipeline into a Kafka Streams {@link KafkaStreamsTranslationContext}. Tests that
   * need the {@link Topology} (e.g. to attach a capture processor before driving) use this and
   * build their own {@link TopologyTestDriver}; simpler tests use {@link #run(Pipeline)}.
   */
  public static KafkaStreamsTranslationContext translate(Pipeline pipeline) {
    KafkaStreamsPipelineOptions options =
        pipeline.getOptions().as(KafkaStreamsPipelineOptions.class);
    // Force every Read.Bounded (including the one Create of 2+ elements expands to) into the
    // deprecated primitive Read the runner translates, instead of the default
    // BoundedSourceAsSDFWrapperFn splittable-DoFn expansion the runner cannot execute yet. This is
    // unconditional — it does not depend on the use_deprecated_read experiment (mentor's steer).
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    JobInfo jobInfo =
        JobInfo.create(
            options.getApplicationId(),
            options.getJobName(),
            "",
            PipelineOptionsTranslation.toProto(options));
    KafkaStreamsTranslationContext context = translator.createTranslationContext(jobInfo, options);
    translator.translate(context, translator.prepareForTranslation(pipelineProto));
    return context;
  }

  /**
   * Translates and drives the pipeline to quiescence through a {@link TopologyTestDriver}. Returns
   * the metrics the SDK harness reported while running (attempted values), so tests can assert on
   * user counters — the same surface PAssert uses to verify its assertions ran.
   */
  public static MetricResults run(Pipeline pipeline) {
    KafkaStreamsTranslationContext context = translate(pipeline);
    Topology topology = context.getTopology();
    try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig(pipeline))) {
      // Fire the Impulse wall-clock punctuator; TopologyTestDriver then flows the records through
      // the whole topology, including looping each internal repartition topic (a sink that is also
      // a source, e.g. the one GroupByKey introduces) back to its source, standing in for the
      // broker. A second advance covers punctuators that need a later tick.
      driver.advanceWallClockTime(Duration.ofSeconds(1));
      driver.advanceWallClockTime(Duration.ofSeconds(1));
    }
    return MetricsContainerStepMap.asAttemptedOnlyMetricResults(
        context.getMetricsContainerStepMap());
  }

  /**
   * The name of some processor node with no successors (a topology leaf). Tests with a single leaf
   * attach a capture processor here to observe what the last stage forwards; if a topology has more
   * than one leaf, which one is returned is unspecified.
   */
  public static String findAnyLeafProcessorName(Topology topology) {
    for (TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
      for (TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof TopologyDescription.Processor && node.successors().isEmpty()) {
          return node.name();
        }
      }
    }
    throw new IllegalStateException("no leaf processor found in topology");
  }

  /** Kafka Streams config for a {@link TopologyTestDriver} built from the pipeline's app id. */
  public static Properties streamsConfig(Pipeline pipeline) {
    String applicationId =
        pipeline.getOptions().as(KafkaStreamsPipelineOptions.class).getApplicationId();
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    props.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    return props;
  }
}
