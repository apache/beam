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

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.instantiateCoder;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Translates the {@code beam:transform:group_by_key:v1} URN — the runner's first stateful,
 * shuffle-bearing transform.
 *
 * <p>This is the simplest GroupByKey: GlobalWindow, default trigger, no allowed lateness (per the
 * plan agreed with the mentor). Each key's values are buffered in a Kafka Streams state store and
 * emitted once as {@code KV<K, Iterable<V>>} when the watermark reaches {@link
 * org.apache.beam.sdk.transforms.windowing.BoundedWindow#TIMESTAMP_MAX_VALUE}.
 *
 * <p>Topology added (the Beam key becomes the Kafka record key so Kafka Streams shuffles by it):
 *
 * <ul>
 *   <li>a {@link ShuffleByKeyProcessor} wired to the input's producer, which sets the Kafka record
 *       key to the encoded Beam key for data records and passes watermark reports through;
 *   <li>a {@link Topology#addSink sink} to an internal repartition topic, with the payload encoded
 *       via {@link KStreamsPayloadSerde} and a {@link GroupByKeyBroadcastPartitioner} that hashes
 *       data by key and fans watermark reports out to every partition;
 *   <li>a {@link Topology#addSource source} reading the repartition topic back;
 *   <li>the {@link GroupByKeyProcessor} plus a persistent state store, wired to the source.
 * </ul>
 *
 * <p>The repartition topic is expected to exist on the broker before the job starts (same
 * pre-create assumption as the Impulse bootstrap topic); auto-creation lands with the AdminClient
 * wiring in a follow-up.
 */
class GroupByKeyTranslator implements PTransformTranslator {

  static final String SHUFFLE_SUFFIX = "-shuffle-by-key";
  static final String SINK_SUFFIX = "-repartition-sink";
  static final String SOURCE_SUFFIX = "-repartition-source";
  static final String STATE_STORE_SUFFIX = "-state";
  static final String REPARTITION_TOPIC_PREFIX = "__beam_gbk_";

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    String inputPCollectionId = Iterables.getOnlyElement(transform.getInputsMap().values());
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());

    @SuppressWarnings({"unchecked", "rawtypes"})
    WindowedValues.WindowedValueCoder<KV<Object, Object>> inputCoder =
        (WindowedValues.WindowedValueCoder)
            instantiateCoder(inputPCollectionId, pipeline.getComponents());
    KvCoder<Object, Object> kvCoder = (KvCoder<Object, Object>) inputCoder.getValueCoder();
    Coder<Object> keyCoder = kvCoder.getKeyCoder();
    // User values may be null; the checker tracks that through to the buffered iterables.
    @SuppressWarnings("unchecked")
    Coder<@Nullable Object> valueCoder =
        (Coder<@Nullable Object>) (Coder<?>) kvCoder.getValueCoder();

    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);

    String shuffleName = transformId + SHUFFLE_SUFFIX;
    String sinkName = transformId + SINK_SUFFIX;
    String sourceName = transformId + SOURCE_SUFFIX;
    String stateStoreName = transformId + STATE_STORE_SUFFIX;
    String repartitionTopic = repartitionTopic(transformId);

    KStreamsPayloadSerde<KV<Object, Object>> payloadSerde = new KStreamsPayloadSerde<>(inputCoder);

    Topology topology = context.getTopology();

    // Re-key data records by the encoded Beam key; pass watermark reports through.
    topology.addProcessor(shuffleName, () -> new ShuffleByKeyProcessor(keyCoder), parentProcessor);

    // Shuffle through the repartition topic: data partitioned by key, watermark broadcast.
    topology.addSink(
        sinkName,
        repartitionTopic,
        Serdes.ByteArray().serializer(),
        payloadSerde.serializer(),
        new GroupByKeyBroadcastPartitioner<>(),
        shuffleName);
    topology.addSource(
        sourceName,
        Serdes.ByteArray().deserializer(),
        payloadSerde.deserializer(),
        repartitionTopic);

    // Buffer values per key and fire KV<K, Iterable<V>> at the terminal watermark. Watermark
    // reports cross the repartition topic unchanged, so they still carry the id of the transform
    // that produced this GroupByKey's input — the parent the shuffle is attached to.
    topology.addProcessor(
        transformId,
        () ->
            new GroupByKeyProcessor(
                stateStoreName,
                transformId,
                ImmutableSet.of(parentProcessor),
                keyCoder,
                valueCoder),
        sourceName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName), Serdes.ByteArray(), Serdes.ByteArray()),
        transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }

  /** The internal repartition topic name for a GroupByKey transform. */
  static String repartitionTopic(String transformId) {
    return REPARTITION_TOPIC_PREFIX + transformId.replaceAll("[^a-zA-Z0-9._-]", "_");
  }
}
