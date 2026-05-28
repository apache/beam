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

import org.apache.beam.model.pipeline.v1.RunnerApi;

/**
 * Translates a single Beam {@link RunnerApi.PTransform} into one or more nodes in the Kafka Streams
 * {@link org.apache.kafka.streams.Topology} held by the {@link KafkaStreamsTranslationContext}.
 */
@FunctionalInterface
public interface PTransformTranslator {

  /**
   * Translates the transform identified by {@code transformId} into nodes on the topology held by
   * {@code context}.
   *
   * @param transformId the id of the transform to translate, as keyed in {@code
   *     pipeline.getComponents().getTransformsMap()}
   * @param pipeline the full pipeline proto, in case the translator needs to walk subcomponents
   *     (coders, windowing strategies, etc.)
   * @param context the shared translation context (topology under construction, producer map, etc.)
   */
  void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context);
}
