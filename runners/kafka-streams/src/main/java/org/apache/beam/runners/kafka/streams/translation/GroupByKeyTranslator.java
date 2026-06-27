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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;

/** Translates the {@code beam:transform:group_by_key:v1} URN. */
class GroupByKeyTranslator implements PTransformTranslator {

  static final String REPARTITION_TOPIC_SUFFIX = "-repartition";
  static final String SINK_SUFFIX = "-sink";
  static final String SOURCE_SUFFIX = "-source";
  static final String EXTRACTOR_SUFFIX = "-extractor";
  static final String STATE_STORE_SUFFIX = "-state";

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    String inputPCollectionId = Iterables.getOnlyElement(transform.getInputsMap().values());
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);

    RehydratedComponents components = RehydratedComponents.forComponents(pipeline.getComponents());
    RunnerApi.PCollection inputPColl =
        pipeline.getComponents().getPcollectionsOrThrow(inputPCollectionId);

    Coder<?> inputCoder;
    try {
      inputCoder = components.getCoder(inputPColl.getCoderId());
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to rehydrate coder for " + inputPCollectionId, e);
    }

    // Input coder should be WindowedValueCoder<KV<K, V>>
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<KV<Object, Object>>> typedInputCoder =
        (Coder<WindowedValue<KV<Object, Object>>>) inputCoder;

    // We extract the KeyCoder to serialize the Kafka key
    @SuppressWarnings("unchecked")
    WindowedValueCoder<KV<Object, Object>> wvCoder =
        (WindowedValueCoder<KV<Object, Object>>) inputCoder;
    @SuppressWarnings("unchecked")
    KvCoder<Object, Object> kvCoder = (KvCoder<Object, Object>) wvCoder.getValueCoder();
    Coder<Object> keyCoder = kvCoder.getKeyCoder();

    KStreamsPayloadSerde<KV<Object, Object>> payloadSerde =
        new KStreamsPayloadSerde<>(typedInputCoder);

    Topology topology = context.getTopology();
    String repartitionTopic = transformId + REPARTITION_TOPIC_SUFFIX;
    String extractorNode = transformId + EXTRACTOR_SUFFIX;
    String sinkNode = transformId + SINK_SUFFIX;
    String sourceNode = transformId + SOURCE_SUFFIX;
    String stateStoreName = transformId + STATE_STORE_SUFFIX;

    // 1. Extractor processor: parses KStreamsPayload, extracts the key and forwards with proper
    // Kafka key
    topology.addProcessor(
        extractorNode, () -> new KeyExtractorProcessor(keyCoder), parentProcessor);

    // 2. Sink node: routes KStreamsPayload to the internal repartition topic
    topology.addSink(
        sinkNode,
        repartitionTopic,
        Serdes.ByteArray().serializer(),
        payloadSerde.serializer(),
        extractorNode);

    // 3. Source node: reads from internal repartition topic
    topology.addSource(
        sourceNode,
        Serdes.ByteArray().deserializer(),
        payloadSerde.deserializer(),
        repartitionTopic);

    // 4. GroupByKey processor: Buffers values and emits grouped elements
    topology.addProcessor(
        transformId,
        () -> new GroupByKeyProcessor(stateStoreName, transformId, typedInputCoder),
        sourceNode);

    // 5. State Store: Buffers elements per key
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName), Serdes.ByteArray(), Serdes.ByteArray()),
        transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }

  /**
   * Processor that extracts the Beam key from the data payload and assigns it as the Kafka record
   * key.
   */
  private static class KeyExtractorProcessor
      implements Processor<
          byte[],
          KStreamsPayload<KV<Object, Object>>,
          byte[],
          KStreamsPayload<KV<Object, Object>>> {

    private final Coder<Object> keyCoder;
    private ProcessorContext<byte[], KStreamsPayload<KV<Object, Object>>> context;

    KeyExtractorProcessor(Coder<Object> keyCoder) {
      this.keyCoder = keyCoder;
    }

    @Override
    public void init(ProcessorContext<byte[], KStreamsPayload<KV<Object, Object>>> context) {
      this.context = context;
    }

    @Override
    public void process(Record<byte[], KStreamsPayload<KV<Object, Object>>> record) {
      KStreamsPayload<KV<Object, Object>> payload = record.value();
      if (payload.isData()) {
        try {
          Object key = payload.getData().getValue().getKey();
          ByteArrayOutputStream os = new ByteArrayOutputStream();
          keyCoder.encode(key, os);
          context.forward(record.withKey(os.toByteArray()));
        } catch (IOException e) {
          throw new RuntimeException("Failed to serialize Beam key for repartitioning", e);
        }
      } else {
        // Watermark payload doesn't have a key, just forward with existing key
        context.forward(record);
      }
    }
  }
}
