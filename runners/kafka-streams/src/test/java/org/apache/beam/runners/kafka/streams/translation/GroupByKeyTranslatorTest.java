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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Tests for {@link GroupByKeyTranslator}. */
public class GroupByKeyTranslatorTest {

  @Test
  public void groupByKeyBuffersAndFiresAtTerminalWatermark() throws Exception {
    KafkaStreamsTranslationContext context = KafkaStreamsPipelineTranslatorTest.newContext();

    Pipeline sdkPipeline = Pipeline.create(PipelineOptionsFactory.create());
    sdkPipeline
        .apply("create", Create.empty(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(sdkPipeline);
    String gbkTransformId = null;
    for (String id : pipeline.getComponents().getTransformsMap().keySet()) {
      if (pipeline
          .getComponents()
          .getTransformsOrThrow(id)
          .getSpec()
          .getUrn()
          .equals("beam:transform:group_by_key:v1")) {
        gbkTransformId = id;
      }
    }

    RunnerApi.PTransform gbkTransform =
        pipeline.getComponents().getTransformsOrThrow(gbkTransformId);
    String inputPCollId = Iterables.getOnlyElement(gbkTransform.getInputsMap().values());
    context.registerPCollectionProducer(inputPCollId, "mock-parent");

    RehydratedComponents components = RehydratedComponents.forComponents(pipeline.getComponents());
    Coder<?> rawInputCoder =
        components.getCoder(
            pipeline.getComponents().getPcollectionsOrThrow(inputPCollId).getCoderId());
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<KV<String, String>>> inputCoder =
        (Coder<WindowedValue<KV<String, String>>>) rawInputCoder;
    KStreamsPayloadSerde<KV<String, String>> payloadSerde = new KStreamsPayloadSerde<>(inputCoder);

    Topology topology = context.getTopology();
    topology.addSource(
        "mock-source",
        Serdes.ByteArray().deserializer(),
        payloadSerde.deserializer(),
        "mock-topic");
    // Just a pass-through to satisfy the parent reference
    topology.addProcessor("mock-parent", PassThroughProcessor::new, "mock-source");

    new GroupByKeyTranslator().translate(gbkTransformId, pipeline, context);

    CapturingProcessor capture = new CapturingProcessor();
    topology.addProcessor("capture", capture, gbkTransformId);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, baseProps())) {
      TestInputTopic<byte[], KStreamsPayload<KV<String, String>>> inputTopic =
          driver.createInputTopic(
              "mock-topic", Serdes.ByteArray().serializer(), payloadSerde.serializer());

      // Send elements
      inputTopic.pipeInput(
          new byte[0], KStreamsPayload.data(WindowedValues.valueInGlobalWindow(KV.of("k1", "v1"))));
      inputTopic.pipeInput(
          new byte[0], KStreamsPayload.data(WindowedValues.valueInGlobalWindow(KV.of("k1", "v2"))));
      inputTopic.pipeInput(
          new byte[0], KStreamsPayload.data(WindowedValues.valueInGlobalWindow(KV.of("k2", "v3"))));

      // No output yet
      assertThat(capture.received.size(), is(0));

      // Send terminal watermark
      inputTopic.pipeInput(
          new byte[0],
          KStreamsPayload.watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis(), 0, 1));
    }

    // Now it should have fired. It fires 1 for each key, plus the watermark
    assertThat(
        capture.received.size(),
        is(3)); // 2 data + 1 watermark (wait, maybe order is deterministic?)

    int dataCount = 0;
    int watermarkCount = 0;
    for (KStreamsPayload<?> payload : capture.received) {
      if (payload.isData()) {
        dataCount++;
        KV<?, ?> kv = (KV<?, ?>) payload.getData().getValue();
        Iterable<?> iter = (Iterable<?>) kv.getValue();
        if ("k1".equals(kv.getKey())) {
          assertThat(Iterables.size(iter), is(2));
        } else if ("k2".equals(kv.getKey())) {
          assertThat(Iterables.size(iter), is(1));
        }
      } else {
        watermarkCount++;
        assertThat(
            payload.asWatermark().getWatermarkMillis(),
            is(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
      }
    }
    assertThat(dataCount, is(2));
    assertThat(watermarkCount, is(1));
  }

  private static Properties baseProps() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-gbk-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    props.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    return props;
  }

  private static class PassThroughProcessor implements Processor<Object, Object, Object, Object> {
    private ProcessorContext<Object, Object> context;

    @Override
    public void init(ProcessorContext<Object, Object> context) {
      this.context = context;
    }

    @Override
    public void process(Record<Object, Object> record) {
      context.forward(record);
    }
  }

  private static class CapturingProcessor
      implements ProcessorSupplier<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {
    final List<KStreamsPayload<?>> received = Collections.synchronizedList(new ArrayList<>());

    @Override
    public Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> get() {
      return new Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>>() {
        @Override
        public void init(@Nullable ProcessorContext<byte[], KStreamsPayload<?>> context) {}

        @Override
        public void process(Record<byte[], KStreamsPayload<?>> record) {
          received.add(record.value());
        }
      };
    }
  }
}
