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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.kafka.streams.Topology;

/**
 * Mutable state shared while translating a portable pipeline into a Kafka Streams {@link Topology}.
 *
 * <p>Holds the topology being built and a {@code PCollection-id → processor-node-name} map so that
 * downstream transforms can wire themselves to the right parent node.
 */
public class KafkaStreamsTranslationContext {

  /** Prefix for the per-job bootstrap topic Impulse reads from. */
  private static final String IMPULSE_BOOTSTRAP_TOPIC_PREFIX = "__beam_impulse_";

  /** Prefix for the per-transform bootstrap topic a primitive Read reads from. */
  private static final String READ_BOOTSTRAP_TOPIC_PREFIX = "__beam_read_";

  /** Characters not legal in a Kafka topic name; a topic's legal set is {@code [a-zA-Z0-9._-]}. */
  private static final Pattern ILLEGAL_TOPIC_CHARS = Pattern.compile("[^a-zA-Z0-9._-]");

  private final JobInfo jobInfo;
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final Topology topology;
  private final Map<String, String> pCollectionIdToProcessorName;
  // Accumulates the Beam metrics reported by the SDK harness, one container per executable stage.
  // Processors update it as bundles complete (in-JVM reference sharing); the pipeline result
  // exposes it as MetricResults. Sharing one container across a stage's parallel tasks is safe and
  // correct: the metric cells are thread-safe (atomic cells in concurrent maps) and the updates are
  // per-bundle final values applied with add semantics, so concurrent tasks accumulate rather than
  // overwrite. Aggregation across multiple runner JVMs is out of scope until the multi-instance
  // work.
  private final MetricsContainerStepMap metricsContainerStepMap = new MetricsContainerStepMap();

  public static KafkaStreamsTranslationContext create(
      JobInfo jobInfo, KafkaStreamsPipelineOptions pipelineOptions) {
    return new KafkaStreamsTranslationContext(jobInfo, pipelineOptions, new Topology());
  }

  static KafkaStreamsTranslationContext createWithTopology(
      JobInfo jobInfo, KafkaStreamsPipelineOptions pipelineOptions, Topology topology) {
    return new KafkaStreamsTranslationContext(jobInfo, pipelineOptions, topology);
  }

  private KafkaStreamsTranslationContext(
      JobInfo jobInfo, KafkaStreamsPipelineOptions pipelineOptions, Topology topology) {
    this.jobInfo = jobInfo;
    this.pipelineOptions = pipelineOptions;
    this.topology = topology;
    this.pCollectionIdToProcessorName = new HashMap<>();
  }

  public JobInfo getJobInfo() {
    return jobInfo;
  }

  public KafkaStreamsPipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  /** Returns the {@link Topology} being built by the translation. */
  public Topology getTopology() {
    return topology;
  }

  /**
   * Returns the job's metrics accumulator: one {@link
   * org.apache.beam.runners.core.metrics.MetricsContainerImpl container} per executable stage,
   * updated by the stage processors as the SDK harness reports bundle metrics, and read by the
   * pipeline result via {@link MetricsContainerStepMap#asAttemptedOnlyMetricResults}.
   */
  public MetricsContainerStepMap getMetricsContainerStepMap() {
    return metricsContainerStepMap;
  }

  /**
   * Registers the processor node that produces the given Beam PCollection. Downstream translators
   * resolve their parent processor names by looking up the input PCollection id.
   */
  public void registerPCollectionProducer(String pCollectionId, String processorName) {
    String existing = pCollectionIdToProcessorName.putIfAbsent(pCollectionId, processorName);
    if (existing != null && !existing.equals(processorName)) {
      throw new IllegalStateException(
          "PCollection "
              + pCollectionId
              + " already produced by processor "
              + existing
              + "; cannot reassign to "
              + processorName);
    }
  }

  /** Returns the processor node name producing the given PCollection. */
  public String getProcessorNameForPCollection(String pCollectionId) {
    String name = pCollectionIdToProcessorName.get(pCollectionId);
    if (name == null) {
      throw new IllegalStateException("No processor registered for PCollection " + pCollectionId);
    }
    return name;
  }

  /** Returns the dedicated bootstrap topic name used by Impulse for this application. */
  public String getImpulseBootstrapTopic() {
    return IMPULSE_BOOTSTRAP_TOPIC_PREFIX + pipelineOptions.getApplicationId();
  }

  /**
   * Returns the dedicated bootstrap topic name a primitive Read reads from. Keyed by transform id
   * (sanitized to Kafka's legal topic-name character set) so multiple Reads — and Impulse — never
   * register the same topic on two source nodes, which Kafka Streams rejects.
   */
  public String getReadBootstrapTopic(String transformId) {
    String sanitizedTransformId = ILLEGAL_TOPIC_CHARS.matcher(transformId).replaceAll("_");
    return READ_BOOTSTRAP_TOPIC_PREFIX
        + pipelineOptions.getApplicationId()
        + "_"
        + sanitizedTransformId;
  }
}
