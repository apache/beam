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

  /** The watermark source stamp a producer reports for an output not consumed by a Flatten. */
  private static final SourceStamp SINGLE_SOURCE = new SourceStamp(0, 1);

  private final JobInfo jobInfo;
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final Topology topology;
  private final Map<String, String> pCollectionIdToProcessorName;
  // PCollection id -> the (sourcePartition, totalPartitions) its producer stamps its watermark
  // with.
  // Only PCollections consumed by a Flatten get a non-default entry (their branch index of the
  // Flatten's fan-in); everything else reports as the single source (0 of 1). See getSourceStamp.
  private final Map<String, SourceStamp> pCollectionIdToSourceStamp;

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
    this.pCollectionIdToSourceStamp = new HashMap<>();
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

  /**
   * Records that the given PCollection is the {@code sourcePartition}-th of {@code totalPartitions}
   * input branches of a Flatten, so its producer stamps its watermark with that identity instead of
   * the default single source. Lets the Flatten's {@link WatermarkManager} tell its branches apart
   * (Kafka Streams does not tell a processor which parent forwarded a record).
   */
  public void registerFlattenSourceStamp(
      String pCollectionId, int sourcePartition, int totalPartitions) {
    pCollectionIdToSourceStamp.put(
        pCollectionId, new SourceStamp(sourcePartition, totalPartitions));
  }

  /**
   * Returns the watermark source stamp a producer should report for the given output PCollection:
   * its Flatten branch identity if it feeds a Flatten (see {@link #registerFlattenSourceStamp}), or
   * the single source {@code (0 of 1)} otherwise.
   */
  public SourceStamp getSourceStamp(String pCollectionId) {
    return pCollectionIdToSourceStamp.getOrDefault(pCollectionId, SINGLE_SOURCE);
  }

  /** A watermark report's source identity: partition {@code sourcePartition} of {@code total}. */
  public static final class SourceStamp {
    private final int sourcePartition;
    private final int totalPartitions;

    SourceStamp(int sourcePartition, int totalPartitions) {
      this.sourcePartition = sourcePartition;
      this.totalPartitions = totalPartitions;
    }

    public int getSourcePartition() {
      return sourcePartition;
    }

    public int getTotalPartitions() {
      return totalPartitions;
    }
  }
}
