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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * Runner-native translator for {@code beam:transform:redistribute_arbitrarily:v1}.
 *
 * <p>The default Beam expansion of {@code Redistribute.arbitrarily()} goes through {@code
 * GroupByKey} for materialization. The Kafka Streams runner does not need to do any actual
 * redistribution in the single-instance topology — Kafka Streams is already handling per-task
 * processing — so we provide a passthrough: the output PCollection is mapped to the same processor
 * that already produces the input PCollection. No topology node is added.
 *
 * <p>The translator is registered in {@link KafkaStreamsPipelineTranslator#knownUrns()} so {@link
 * org.apache.beam.sdk.util.construction.graph.TrivialNativeTransformExpander} strips the
 * sub-transforms of {@code Redistribute} before the fuser runs. That keeps the Redistribute
 * boundary intact and lets the fuser split adjacent stages correctly.
 *
 * <p>The keyed variant ({@code redistribute_by_key:v1}) is intentionally not handled here: it
 * implies a rehash/partition step that the runner can implement on top of GroupByKey when that
 * lands, but cannot reasonably no-op the way the arbitrary variant can.
 */
class RedistributeTranslator implements PTransformTranslator {

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    String inputPCollectionId = Iterables.getOnlyElement(transform.getInputsMap().values());
    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
    // Passthrough: downstream lookups for the output PCollection resolve to the producer of the
    // input PCollection. No KS Processor / state store / source is added.
    context.registerPCollectionProducer(outputPCollectionId, parentProcessor);
  }
}
