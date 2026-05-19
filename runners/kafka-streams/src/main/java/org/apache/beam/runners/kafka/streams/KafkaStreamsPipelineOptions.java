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

/** Pipeline options for the Kafka Streams runner. */
public interface KafkaStreamsPipelineOptions extends PortablePipelineOptions {

  @Description("Comma-separated list of host:port Kafka brokers used by the Kafka Streams client.")
  @Default.String("localhost:9092")
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @Description(
      "Kafka Streams application.id (must be unique for each distinct topology using the same "
          + "input topics in a Kafka cluster).")
  @Default.String("beam-kafka-streams-runner")
  String getApplicationId();

  void setApplicationId(String applicationId);

  @Description(
      "Kafka Streams processing.guarantee setting, for example at_least_once or exactly_once_v2.")
  @Default.String("exactly_once_v2")
  String getProcessingGuarantee();

  void setProcessingGuarantee(String processingGuarantee);

  @Description("Soft cap on the number of elements per bundle.")
  @Default.Integer(1000)
  int getMaxBundleSize();

  void setMaxBundleSize(int maxBundleSize);

  @Description("Soft cap on bundle wall-clock duration in milliseconds.")
  @Default.Integer(1000)
  int getMaxBundleTimeMs();

  void setMaxBundleTimeMs(int maxBundleTimeMs);

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
