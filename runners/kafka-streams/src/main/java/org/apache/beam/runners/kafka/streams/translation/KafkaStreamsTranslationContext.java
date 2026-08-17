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

  /**
   * How many partitions the transform producing each PCollection runs across. A PCollection that
   * has not been registered is produced by a single instance; only a shuffle raises the count.
   */
  private final Map<String, Integer> pCollectionIdToPartitionCount = new HashMap<>();
  // Beam metrics from the SDK harness, one container per stage. Sharing a container across a
  // stage's parallel tasks is safe: the cells are thread-safe and updates add rather than
  // overwrite. Aggregating across runner JVMs is out of scope for now.
  private final MetricsContainerStepMap metricsContainerStepMap = new MetricsContainerStepMap();

  // Scoped to this pipeline rather than the JVM: the job server runs several jobs in one process,
  // and a shared tracker would let one job's completion stop another.
  private final TerminationTracker terminationTracker = new TerminationTracker();

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

  public Topology getTopology() {
    return topology;
  }

  /** One container per stage, updated by the processors and read by the pipeline result. */
  public MetricsContainerStepMap getMetricsContainerStepMap() {
    return metricsContainerStepMap;
  }

  /**
   * Processors report to it at the terminal watermark; the runner stops the client once all have.
   */
  public TerminationTracker getTerminationTracker() {
    return terminationTracker;
  }

  /** Downstream translators resolve their parent node by looking up the input PCollection id. */
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

  /**
   * The {@code totalSourcePartitions} this PCollection's watermark reports carry, which a
   * downstream {@link WatermarkAggregator} waits on. It changes only at a shuffle.
   */
  public void registerPCollectionPartitionCount(String pCollectionId, int partitionCount) {
    pCollectionIdToPartitionCount.put(pCollectionId, partitionCount);
  }

  /** One unless a shuffle upstream raised it; never less, as --internalParallelism is validated. */
  public int getPartitionCount(String pCollectionId) {
    return pCollectionIdToPartitionCount.getOrDefault(pCollectionId, 1);
  }

  /** Returns the processor node name producing the given PCollection. */
  public String getProcessorNameForPCollection(String pCollectionId) {
    String name = pCollectionIdToProcessorName.get(pCollectionId);
    if (name == null) {
      throw new IllegalStateException("No processor registered for PCollection " + pCollectionId);
    }
    return name;
  }

  /**
   * Keyed by transform id, sanitized to Kafka's legal topic characters: a pipeline can hold several
   * Impulses, and Kafka Streams rejects the same topic on two source nodes.
   */
  public String getImpulseBootstrapTopic(String transformId) {
    String sanitizedTransformId = ILLEGAL_TOPIC_CHARS.matcher(transformId).replaceAll("_");
    return IMPULSE_BOOTSTRAP_TOPIC_PREFIX
        + pipelineOptions.getApplicationId()
        + "_"
        + sanitizedTransformId;
  }

  /** Keyed by transform id, for the same reason as {@link #getImpulseBootstrapTopic}. */
  public String getReadBootstrapTopic(String transformId) {
    String sanitizedTransformId = ILLEGAL_TOPIC_CHARS.matcher(transformId).replaceAll("_");
    return READ_BOOTSTRAP_TOPIC_PREFIX
        + pipelineOptions.getApplicationId()
        + "_"
        + sanitizedTransformId;
  }

  /**
   * Returns the name of a state store belonging to a transform.
   *
   * <p>The transform id is sanitized to Kafka's legal topic-name characters even though a store
   * name is not itself a topic: Kafka Streams names a persistent store's changelog topic after the
   * store, so a transform whose name contains a character a topic may not — which is ordinary,
   * {@code CombinePerKey(MeanCombineFn)/Group} is a Beam transform name — would fail at runtime
   * when the changelog is created.
   *
   * <p>Two transform ids differing only in characters that are replaced would sanitize to one name.
   * Kafka Streams rejects a store name that is already taken when the topology is built, so that
   * surfaces as a failure to start rather than as two transforms quietly sharing state.
   */
  public static String getStoreName(String transformId, String suffix) {
    return ILLEGAL_TOPIC_CHARS.matcher(transformId).replaceAll("_") + suffix;
  }
}
