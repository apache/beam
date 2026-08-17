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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

/**
 * Translates the {@code beam:transform:impulse:v1} URN.
 *
 * <p>Adds three nodes: a {@code byte[]} source bound to a per-transform bootstrap topic, which
 * exists only because Kafka Streams refuses to start a topology with no source topic and whose
 * records {@link ImpulseProcessor} ignores; the processor itself, which fires a one-shot wall-clock
 * punctuator and emits one empty data payload followed by a terminal watermark; and a persistent
 * state store recording whether it already fired, so a restart does not duplicate the impulse.
 *
 * <p>The output PCollection is registered with the translation context so later translators can
 * wire to this node by id. The bootstrap topic itself is created before startup by {@link
 * org.apache.beam.runners.kafka.streams.KafkaStreamsTopicManager}.
 */
class ImpulseTranslator implements PTransformTranslator {

  static final String SOURCE_SUFFIX = "-source";
  static final String STATE_STORE_SUFFIX = "-state";

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    // Impulse produces exactly one output PCollection. This is the produced-outputs map on the
    // transform, not the consumer count — downstream transforms that consume this PCollection are
    // modeled as separate PTransforms whose `inputs` reference the same PCollection id, and they
    // are wired up by their own translators. Iterables.getOnlyElement throws a clear
    // IllegalArgumentException if the proto is malformed.
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());

    Topology topology = context.getTopology();
    String sourceNodeName = transformId + SOURCE_SUFFIX;
    String stateStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, STATE_STORE_SUFFIX);
    String bootstrapTopic = context.getImpulseBootstrapTopic(transformId);

    topology.addSource(
        sourceNodeName,
        Serdes.ByteArray().deserializer(),
        Serdes.ByteArray().deserializer(),
        bootstrapTopic);
    topology.addProcessor(
        transformId,
        () -> new ImpulseProcessor(stateStoreName, transformId, context.getTerminationTracker()),
        sourceNodeName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName), Serdes.String(), Serdes.Boolean()),
        transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }
}
