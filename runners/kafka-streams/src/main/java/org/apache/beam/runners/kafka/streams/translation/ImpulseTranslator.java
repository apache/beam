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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

/**
 * Translates the {@code beam:transform:impulse:v1} URN.
 *
 * <p>Adds three nodes to the Kafka Streams {@link Topology}:
 *
 * <ul>
 *   <li>A {@code byte[]} source bound to a dedicated per-transform bootstrap topic (see {@link
 *       KafkaStreamsTranslationContext#getImpulseBootstrapTopic}). Kafka Streams refuses to start a
 *       topology that has no real source topic, so the bootstrap topic exists purely to satisfy
 *       that requirement — records published to it are ignored by {@link ImpulseProcessor}.
 *   <li>The {@link ImpulseProcessor} itself, which schedules a one-shot wall-clock punctuator on
 *       {@code init} and emits a single empty data {@link KStreamsPayload} followed by a terminal
 *       watermark payload at {@link
 *       org.apache.beam.sdk.transforms.windowing.BoundedWindow#TIMESTAMP_MAX_VALUE}.
 *   <li>A per-processor {@link KeyValueBytesStoreSupplier persistent state store} that records
 *       whether the impulse has already fired so task restarts do not duplicate it.
 * </ul>
 *
 * <p>The processor's output PCollection is registered with the translation context so subsequent
 * translators can wire themselves to this node by id.
 *
 * <p><b>Bootstrap topic lifecycle:</b> this translator does <em>not</em> auto-create the bootstrap
 * topic. The topic is expected to exist on the broker before the job starts; otherwise Kafka
 * Streams raises {@code MissingSourceTopicException} on startup. The auto-create-vs-pre-create
 * decision (design doc §12.1) is deferred to a follow-up sub-issue along with the {@code
 * AdminClient} wiring; pre-creation is sufficient for the {@code TopologyTestDriver}-based unit
 * tests in this PR.
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
    String stateStoreName = transformId + STATE_STORE_SUFFIX;
    String bootstrapTopic = context.getImpulseBootstrapTopic(transformId);

    topology.addSource(
        sourceNodeName,
        Serdes.ByteArray().deserializer(),
        Serdes.ByteArray().deserializer(),
        bootstrapTopic);
    topology.addProcessor(
        transformId, () -> new ImpulseProcessor(stateStoreName, transformId), sourceNodeName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName), Serdes.String(), Serdes.Boolean()),
        transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }
}
