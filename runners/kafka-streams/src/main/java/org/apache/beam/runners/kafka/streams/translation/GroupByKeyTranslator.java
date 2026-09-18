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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
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
 * <p>Windowing and triggering run through Beam's {@link
 * org.apache.beam.runners.core.ReduceFnRunner} inside {@link WindowedGroupByKeyProcessor}, as the
 * Flink and Spark portable runners do, so fixed and sliding windows, the default trigger, allowed
 * lateness and timestamp combiners all work. The input's windowing strategy is hydrated from the
 * pipeline proto and handed to the processor.
 *
 * <p>The Beam key becomes the Kafka record key so Kafka Streams shuffles by it. The topology is a
 * {@link ShuffleByKeyProcessor} that sets that key and passes watermark reports through, a sink to
 * an internal repartition topic using a {@link GroupByKeyBroadcastPartitioner} that hashes data by
 * key and fans watermarks out to every partition, a source reading that topic back, and the {@link
 * WindowedGroupByKeyProcessor} with its state and timer stores.
 */
class GroupByKeyTranslator implements PTransformTranslator {

  static final String SHUFFLE_SUFFIX = "-shuffle-by-key";
  static final String SINK_SUFFIX = "-repartition-sink";
  static final String SOURCE_SUFFIX = "-repartition-source";
  static final String STATE_STORE_SUFFIX = "-state";
  static final String HOLDS_INDEX_STORE_SUFFIX = "-holds-index";
  static final String TIMER_STORE_SUFFIX = "-timers";
  static final String TIMER_INDEX_STORE_SUFFIX = "-timers-index";
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

    WindowingStrategy<?, BoundedWindow> windowingStrategy =
        hydrateWindowingStrategy(pipeline, inputPCollectionId);

    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);
    // The shuffle is what changes the parallelism: everything from the repartition topic onwards
    // runs one task per partition of it.
    int partitionCount = context.getPipelineOptions().getInternalParallelism();

    String shuffleName = transformId + SHUFFLE_SUFFIX;
    String sinkName = transformId + SINK_SUFFIX;
    String sourceName = transformId + SOURCE_SUFFIX;
    String stateStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, STATE_STORE_SUFFIX);
    String holdsIndexStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, HOLDS_INDEX_STORE_SUFFIX);
    String timerStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, TIMER_STORE_SUFFIX);
    String timerIndexStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, TIMER_INDEX_STORE_SUFFIX);
    String repartitionTopic =
        repartitionTopic(transformId, context.getPipelineOptions().getApplicationId());

    KStreamsPayloadSerde<KV<Object, Object>> payloadSerde = new KStreamsPayloadSerde<>(inputCoder);

    Topology topology = context.getTopology();

    // Re-key data records by the encoded Beam key; pass watermark reports through.
    // The shuffle runs in the upstream transform's task, so it relabels each report with that
    // transform's instance identity before the sink broadcasts it to every partition.
    int upstreamPartitionCount = context.getPartitionCount(inputPCollectionId);
    topology.addProcessor(
        shuffleName,
        () ->
            new ShuffleByKeyProcessor(
                keyCoder, upstreamPartitionCount, shuffleName, context.getTerminationTracker()),
        parentProcessor);

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

    // Group by key and window through Beam's ReduceFnRunner, backed by the state and timer stores.
    // Watermark reports cross the repartition topic unchanged, so they still carry the id of the
    // transform that produced this GroupByKey's input — the parent the shuffle is attached to.
    topology.addProcessor(
        transformId,
        () ->
            new WindowedGroupByKeyProcessor<Object, @Nullable Object, BoundedWindow>(
                stateStoreName,
                holdsIndexStoreName,
                timerStoreName,
                timerIndexStoreName,
                transformId,
                ImmutableSet.of(parentProcessor),
                keyCoder,
                valueCoder,
                windowingStrategy,
                context.getPipelineOptions(),
                context.getTerminationTracker()),
        sourceName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName), Serdes.ByteArray(), Serdes.ByteArray()),
        transformId);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(timerStoreName), Serdes.ByteArray(), Serdes.ByteArray()),
        transformId);
    // Indexes ordered by timestamp, so due timers and the minimum watermark hold are range scans
    // rather than scans of every timer or every held window.
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(timerIndexStoreName),
            Serdes.ByteArray(),
            Serdes.ByteArray()),
        transformId);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(holdsIndexStoreName),
            Serdes.ByteArray(),
            Serdes.ByteArray()),
        transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
    context.registerPCollectionPartitionCount(outputPCollectionId, partitionCount);
  }

  /** Hydrates the input PCollection's windowing strategy from the pipeline proto. */
  private static WindowingStrategy<?, BoundedWindow> hydrateWindowingStrategy(
      RunnerApi.Pipeline pipeline, String inputPCollectionId) {
    RunnerApi.Components components = pipeline.getComponents();
    String windowingStrategyId =
        components.getPcollectionsOrThrow(inputPCollectionId).getWindowingStrategyId();
    try {
      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> strategy =
          (WindowingStrategy<?, BoundedWindow>)
              WindowingStrategyTranslation.fromProto(
                  components.getWindowingStrategiesOrThrow(windowingStrategyId),
                  RehydratedComponents.forComponents(components));
      return strategy;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to hydrate GroupByKey windowing strategy " + windowingStrategyId, e);
    }
  }

  /**
   * The internal repartition topic name for a GroupByKey transform.
   *
   * <p>Namespaced by application id, as the Impulse and Read bootstrap topics already are.
   * Transform ids come from the pipeline's structure, so two jobs running the same pipeline would
   * otherwise shuffle through the same topic and read each other's data — and, because the topic is
   * created only if it does not already exist, the second job would silently inherit the first
   * job's partition count rather than the one it asked for.
   */
  static String repartitionTopic(String transformId, String applicationId) {
    return REPARTITION_TOPIC_PREFIX
        + applicationId.replaceAll("[^a-zA-Z0-9._-]", "_")
        + "_"
        + transformId.replaceAll("[^a-zA-Z0-9._-]", "_");
  }
}
