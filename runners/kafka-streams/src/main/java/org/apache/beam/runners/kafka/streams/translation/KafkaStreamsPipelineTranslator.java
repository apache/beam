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

import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;

/**
 * Translates a portable Beam pipeline into a Kafka Streams {@code Topology}.
 *
 * <p>The initial implementation only validates the graph and fails fast with an explicit message
 * for transforms that are not yet supported.
 */
public class KafkaStreamsPipelineTranslator {

  public KafkaStreamsTranslationContext createTranslationContext(
      JobInfo jobInfo, KafkaStreamsPipelineOptions pipelineOptions) {
    return KafkaStreamsTranslationContext.create(jobInfo, pipelineOptions);
  }

  /** Returns the pipeline to translate (placeholder for future fusion / expansion steps). */
  public RunnerApi.Pipeline prepareForTranslation(RunnerApi.Pipeline pipeline) {
    return pipeline;
  }

  /**
   * Translates the pipeline. Throws {@link UnsupportedOperationException} with a clear URN message
   * for the first unsupported primitive encountered.
   */
  public void translate(KafkaStreamsTranslationContext context, RunnerApi.Pipeline pipeline) {
    Map<String, RunnerApi.PTransform> transforms = pipeline.getComponents().getTransformsMap();
    TreeMap<String, RunnerApi.PTransform> ordered = new TreeMap<>(transforms);
    for (Map.Entry<String, RunnerApi.PTransform> entry : ordered.entrySet()) {
      RunnerApi.PTransform transform = entry.getValue();
      if (!transform.hasSpec()) {
        continue;
      }
      String urn = transform.getSpec().getUrn();
      if (urn.isEmpty()) {
        continue;
      }
      throw new UnsupportedOperationException(
          "No translator registered for URN "
              + urn
              + " (jobId="
              + context.getJobInfo().jobId()
              + ")");
    }
    throw new UnsupportedOperationException(
        "No translator registered for pipeline (no transform URNs found)");
  }
}
