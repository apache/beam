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
package org.apache.beam.sdk.io.kafka;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExternalConfigurationPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.reflect.Whitebox;

/** Tests for building {@link KafkaIO} externally via the ExpansionService. */
@RunWith(JUnit4.class)
public class KafkaIOExternalTest {
  @Test
  public void testConstructKafkaRead() throws Exception {
    List<String> topics = ImmutableList.of("topic1", "topic2");
    String keyDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    String valueDeserializer = "org.apache.kafka.common.serialization.LongDeserializer";
    ImmutableMap<String, String> consumerConfig =
        ImmutableMap.<String, String>builder()
            .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server1:port,server2:port")
            .put("key2", "value2")
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
            .build();
    Long startReadTime = 100L;

    ExternalTransforms.ExternalConfigurationPayload payload =
        encodeRow(
            Row.withSchema(
                    Schema.of(
                        Field.of("topics", FieldType.array(FieldType.STRING)),
                        Field.of(
                            "consumer_config", FieldType.map(FieldType.STRING, FieldType.STRING)),
                        Field.of("key_deserializer", FieldType.STRING),
                        Field.of("value_deserializer", FieldType.STRING),
                        Field.of("start_read_time", FieldType.INT64)))
                .withFieldValue("topics", topics)
                .withFieldValue("consumer_config", consumerConfig)
                .withFieldValue("key_deserializer", keyDeserializer)
                .withFieldValue("value_deserializer", valueDeserializer)
                .withFieldValue("start_read_time", startReadTime)
                .build());

    RunnerApi.Components defaultInstance = RunnerApi.Components.getDefaultInstance();
    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(defaultInstance)
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:external:java:kafka:read:v1")
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;
    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.contains(
            "test_namespacetest/KafkaIO.Read", "test_namespacetest/Remove Kafka Metadata"));
    assertThat(transform.getInputsCount(), Matchers.is(0));
    assertThat(transform.getOutputsCount(), Matchers.is(1));

    RunnerApi.PTransform kafkaComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));

    // KafkaIO.Read should be expanded into SDF transform.
    assertThat(
        kafkaComposite.getSubtransformsList(),
        Matchers.contains(
            "test_namespacetest/KafkaIO.Read/Impulse",
            "test_namespacetest/KafkaIO.Read/ParDo(GenerateKafkaSourceDescriptor)",
            "test_namespacetest/KafkaIO.Read/KafkaIO.ReadSourceDescriptors"));

    // Verify the consumerConfig and topics are populated correctly to
    // GenerateKafkaSourceDescriptor.
    RunnerApi.PTransform generateParDo =
        result.getComponents().getTransformsOrThrow(kafkaComposite.getSubtransforms(1));
    KafkaIO.Read.GenerateKafkaSourceDescriptor generateDoFn =
        (KafkaIO.Read.GenerateKafkaSourceDescriptor)
            ParDoTranslation.getDoFn(
                RunnerApi.ParDoPayload.parseFrom(
                    result
                        .getComponents()
                        .getTransformsOrThrow(generateParDo.getSubtransforms(0))
                        .getSpec()
                        .getPayload()));
    assertThat(generateDoFn.consumerConfig, Matchers.is(consumerConfig));
    assertThat(generateDoFn.topics, Matchers.is(topics));

    // Verify that the consumerConfig, keyDeserializerProvider, valueDeserializerProvider are
    // populated correctly to the SDF.
    RunnerApi.PTransform readViaSDF =
        result.getComponents().getTransformsOrThrow(kafkaComposite.getSubtransforms(2));
    RunnerApi.PTransform subTransform =
        result.getComponents().getTransformsOrThrow(readViaSDF.getSubtransforms(0));

    ReadFromKafkaDoFn readSDF =
        (ReadFromKafkaDoFn)
            ParDoTranslation.getDoFn(
                RunnerApi.ParDoPayload.parseFrom(
                    result
                        .getComponents()
                        .getTransformsOrThrow(subTransform.getSubtransforms(0))
                        .getSpec()
                        .getPayload()));

    assertThat(readSDF.consumerConfig, Matchers.is(consumerConfig));
    assertThat(
        readSDF
            .keyDeserializerProvider
            .getDeserializer(readSDF.consumerConfig, true)
            .getClass()
            .getName(),
        Matchers.is(keyDeserializer));
    assertThat(
        readSDF
            .valueDeserializerProvider
            .getDeserializer(readSDF.consumerConfig, false)
            .getClass()
            .getName(),
        Matchers.is(valueDeserializer));
  }

  @Test
  public void testConstructKafkaWrite() throws Exception {
    String topic = "topic";
    String keySerializer = "org.apache.kafka.common.serialization.ByteArraySerializer";
    String valueSerializer = "org.apache.kafka.common.serialization.LongSerializer";
    ImmutableMap<String, String> producerConfig =
        ImmutableMap.<String, String>builder()
            .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "server1:port,server2:port")
            .put("retries", "3")
            .build();

    ExternalTransforms.ExternalConfigurationPayload payload =
        encodeRow(
            Row.withSchema(
                    Schema.of(
                        Field.of("topic", FieldType.STRING),
                        Field.of(
                            "producer_config", FieldType.map(FieldType.STRING, FieldType.STRING)),
                        Field.of("key_serializer", FieldType.STRING),
                        Field.of("value_serializer", FieldType.STRING)))
                .withFieldValue("topic", topic)
                .withFieldValue("producer_config", producerConfig)
                .withFieldValue("key_serializer", keySerializer)
                .withFieldValue("value_serializer", valueSerializer)
                .build());

    Pipeline p = Pipeline.create();
    p.apply(Impulse.create()).apply(WithKeys.of("key"));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    String inputPCollection =
        Iterables.getOnlyElement(
            Iterables.getLast(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .putInputs("input", inputPCollection)
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:external:java:kafka:write:v1")
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;
    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.contains(
            "test_namespacetest/Kafka ProducerRecord", "test_namespacetest/KafkaIO.WriteRecords"));
    assertThat(transform.getInputsCount(), Matchers.is(1));
    assertThat(transform.getOutputsCount(), Matchers.is(0));

    RunnerApi.PTransform writeComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(1));
    RunnerApi.PTransform writeParDo =
        result
            .getComponents()
            .getTransformsOrThrow(
                result
                    .getComponents()
                    .getTransformsOrThrow(writeComposite.getSubtransforms(0))
                    .getSubtransforms(0));

    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.parseFrom(writeParDo.getSpec().getPayload());
    DoFn kafkaWriter = ParDoTranslation.getDoFn(parDoPayload);
    assertThat(kafkaWriter, Matchers.instanceOf(KafkaWriter.class));
    KafkaIO.WriteRecords spec =
        (KafkaIO.WriteRecords) Whitebox.getInternalState(kafkaWriter, "spec");

    assertThat(spec.getProducerConfig(), Matchers.is(producerConfig));
    assertThat(spec.getTopic(), Matchers.is(topic));
    assertThat(spec.getKeySerializer().getName(), Matchers.is(keySerializer));
    assertThat(spec.getValueSerializer().getName(), Matchers.is(valueSerializer));
  }

  private static byte[] listAsBytes(List<String> stringList) throws IOException {
    IterableCoder<String> coder = IterableCoder.of(StringUtf8Coder.of());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    coder.encode(stringList, baos);
    return baos.toByteArray();
  }

  private static byte[] mapAsBytes(Map<String, String> stringMap) throws IOException {
    IterableCoder<KV<String, String>> coder =
        IterableCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    List<KV<String, String>> stringList =
        stringMap.entrySet().stream()
            .map(kv -> KV.of(kv.getKey(), kv.getValue()))
            .collect(Collectors.toList());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    coder.encode(stringList, baos);
    return baos.toByteArray();
  }

  private static byte[] encodeString(String str) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringUtf8Coder.of().encode(str, baos);
    return baos.toByteArray();
  }

  private static ExternalConfigurationPayload encodeRow(Row row) {
    ByteString.Output outputStream = ByteString.newOutput();
    try {
      SchemaCoder.of(row.getSchema()).encode(row, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ExternalConfigurationPayload.newBuilder()
        .setSchema(SchemaTranslation.schemaToProto(row.getSchema(), true))
        .setPayload(outputStream.toByteString())
        .build();
  }

  private static class TestStreamObserver<T> implements StreamObserver<T> {

    private T result;

    @Override
    public void onNext(T t) {
      result = t;
    }

    @Override
    public void onError(Throwable throwable) {
      throw new RuntimeException("Should not happen", throwable);
    }

    @Override
    public void onCompleted() {}
  }
}
