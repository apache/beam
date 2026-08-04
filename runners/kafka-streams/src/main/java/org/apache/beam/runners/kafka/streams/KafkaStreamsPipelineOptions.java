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

import java.nio.file.Paths;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options for the Kafka Streams runner. */
public interface KafkaStreamsPipelineOptions extends PortablePipelineOptions {

  @Description("Comma-separated list of host:port Kafka brokers used by the Kafka Streams client.")
  @Default.String("localhost:9092")
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @Description(
      "Kafka Streams application.id (must be unique for each distinct topology using the same "
          + "input topics in a Kafka cluster). Must be specified explicitly: a shared default "
          + "would let concurrent jobs collide on the same Kafka Streams consumer group.")
  @Validation.Required
  String getApplicationId();

  void setApplicationId(String applicationId);

  @Description("Soft cap on the number of elements per bundle.")
  @Default.Integer(1000)
  int getMaxBundleSize();

  void setMaxBundleSize(int maxBundleSize);

  @Description(
      "Intended cap on how long a bundle may stay open, in milliseconds. NOT APPLIED YET: closing a"
          + " bundle from a wall-clock punctuator made a pipeline with two chained GroupByKeys"
          + " across several partitions emit its groups repeatedly against a real broker, so only"
          + " the element-count bound is enforced for now. See"
          + " https://github.com/apache/beam/issues/18479.")
  @Default.Integer(1000)
  int getMaxBundleTimeMs();

  void setMaxBundleTimeMs(int maxBundleTimeMs);

  @Description(
      "How many partitions the runner gives the internal topics it creates to shuffle a pipeline"
          + " through, which is the parallelism the shuffled parts of that pipeline can reach. A"
          + " GroupByKey runs one task per partition of its repartition topic, so this is the"
          + " number of instances its state and its downstream stages are spread over. Must be at"
          + " least 1.")
  @Default.Integer(1)
  int getInternalParallelism();

  void setInternalParallelism(int internalParallelism);

  @Description("Replication factor for the internal topics the runner creates for a pipeline.")
  @Default.Short(1)
  short getTopicReplicationFactor();

  void setTopicReplicationFactor(short topicReplicationFactor);

  @Description(
      "How many non-empty polls of an unbounded source to make before storing its checkpoint mark."
          + " Taking a mark can be costly for some sources, so it is not worth doing on every poll;"
          + " the cost of a larger value is that more elements are replayed after a restart, since"
          + " the reader resumes from the last mark that was stored.")
  @Default.Integer(10)
  int getReadCheckpointNumBundles();

  void setReadCheckpointNumBundles(int readCheckpointNumBundles);

  @Description("Directory where Kafka Streams stores local state.")
  @Default.InstanceFactory(StateDirDefaultFactory.class)
  String getStateDir();

  void setStateDir(String stateDir);

  /**
   * Default {@link #getStateDir()} under the JVM temp directory.
   *
   * <p>The job name is included in the path so that multiple pipelines running on the same host
   * (e.g. parallel tests) do not collide on the same Kafka Streams state directory and trigger a
   * {@code LockException}.
   */
  class StateDirDefaultFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return Paths.get(
              System.getProperty("java.io.tmpdir"),
              "beam-kafka-streams-state",
              options.getJobName())
          .toString();
    }
  }
}
