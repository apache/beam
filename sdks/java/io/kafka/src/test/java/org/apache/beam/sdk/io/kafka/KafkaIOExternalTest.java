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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExternalConfigurationPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.io.kafka.KafkaIO.ByteArrayKafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaIO.RowsWithMetadata;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.hamcrest.Matchers;
import org.hamcrest.text.MatchesPattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.reflect.Whitebox;

/** Tests for building {@link KafkaIO} externally via the ExpansionService. */
@RunWith(JUnit4.class)
public class KafkaIOExternalTest {

  private void verifyKafkaReadComposite(
      RunnerApi.PTransform kafkaSDFReadComposite, ExpansionApi.ExpansionResponse result)
      throws Exception {
    assertThat(
        kafkaSDFReadComposite.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*Impulse.*")));
    assertThat(
        kafkaSDFReadComposite.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*GenerateKafkaSourceDescriptor.*")));
    assertThat(
        kafkaSDFReadComposite.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*ReadSourceDescriptors.*")));
    RunnerApi.PTransform kafkaSdfParDo =
        result.getComponents().getTransformsOrThrow(kafkaSDFReadComposite.getSubtransforms(2));
    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.parseFrom(kafkaSdfParDo.getSpec().getPayload());
    assertNotNull(parDoPayload.getRestrictionCoderId());
  }

  @Test
  public void testConstructKafkaRead() throws Exception {
    List<String> topics = ImmutableList.of("topic1", "topic2");
    String keyDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    String valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
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
                        Field.of("start_read_time", FieldType.INT64),
                        Field.of("commit_offset_in_finalize", FieldType.BOOLEAN),
                        Field.of("timestamp_policy", FieldType.STRING),
                        Field.of("consumer_polling_timeout", FieldType.INT64),
                        Field.of("redistribute_num_keys", FieldType.INT32),
                        Field.of("redistribute", FieldType.BOOLEAN),
                        Field.of("allow_duplicates", FieldType.BOOLEAN)))
                .withFieldValue("topics", topics)
                .withFieldValue("consumer_config", consumerConfig)
                .withFieldValue("key_deserializer", keyDeserializer)
                .withFieldValue("value_deserializer", valueDeserializer)
                .withFieldValue("start_read_time", startReadTime)
                .withFieldValue("commit_offset_in_finalize", false)
                .withFieldValue("timestamp_policy", "ProcessingTime")
                .withFieldValue("consumer_polling_timeout", 5L)
                .withFieldValue("redistribute_num_keys", 0)
                .withFieldValue("redistribute", false)
                .withFieldValue("allow_duplicates", false)
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
                            .setUrn(
                                org.apache.beam.sdk.io.kafka.KafkaIO.Read.External
                                    .URN_WITH_METADATA)
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();
    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);
    ExpansionApi.ExpansionResponse result = observer.result;
    RunnerApi.PTransform transform = result.getTransform();
    System.out.println("xxx : " + result.toString());
    assertThat(
        transform.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*KafkaIO-Read.*")));
    assertThat(
        transform.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*Convert-to-ExternalKafkaRecord.*")));
    assertThat(
        transform.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*Convert-ConvertTransform.*")));
    assertThat(transform.getInputsCount(), Matchers.is(0));
    assertThat(transform.getOutputsCount(), Matchers.is(1));

    RunnerApi.PTransform kafkaReadComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));

    verifyKafkaReadComposite(
        result.getComponents().getTransformsOrThrow(kafkaReadComposite.getSubtransforms(0)),
        result);
  }

  @Test
  public void testKafkaRecordToExternalKafkaRecord() throws Exception {
    RecordHeaders headers = new RecordHeaders();
    headers.add("dummyHeaderKey", "dummyHeaderVal".getBytes(StandardCharsets.UTF_8));
    KafkaRecord<byte[], byte[]> kafkaRecord =
        new KafkaRecord<>(
            "dummyTopic",
            111,
            222,
            12345,
            KafkaTimestampType.LOG_APPEND_TIME,
            headers,
            "dummyKey".getBytes(StandardCharsets.UTF_8),
            "dummyValue".getBytes(StandardCharsets.UTF_8));

    ByteArrayKafkaRecord byteArrayKafkaRecord = RowsWithMetadata.toExternalKafkaRecord(kafkaRecord);

    assertEquals("dummyTopic", byteArrayKafkaRecord.topic);
    assertEquals(111, byteArrayKafkaRecord.partition);
    assertEquals(222, byteArrayKafkaRecord.offset);
    assertEquals(12345, byteArrayKafkaRecord.timestamp);
    assertEquals(KafkaTimestampType.LOG_APPEND_TIME.id, byteArrayKafkaRecord.timestampTypeId);
    assertEquals(KafkaTimestampType.LOG_APPEND_TIME.name, byteArrayKafkaRecord.timestampTypeName);
    assertEquals("dummyKey", new String(byteArrayKafkaRecord.key, "UTF-8"));
    assertEquals("dummyValue", new String(byteArrayKafkaRecord.value, "UTF-8"));
    assertEquals(1, byteArrayKafkaRecord.headers.size());
    assertEquals("dummyHeaderKey", byteArrayKafkaRecord.headers.get(0).key);
    assertEquals("dummyHeaderVal", new String(byteArrayKafkaRecord.headers.get(0).value, "UTF-8"));
  }

  @Test
  public void testKafkaRecordToExternalKafkaRecordWithNullKeyAndValue() throws Exception {
    RecordHeaders headers = new RecordHeaders();
    headers.add("dummyHeaderKey", "dummyHeaderVal".getBytes(StandardCharsets.UTF_8));
    KafkaRecord<byte[], byte[]> kafkaRecord =
        new KafkaRecord<>(
            "dummyTopic", 111, 222, 12345, KafkaTimestampType.LOG_APPEND_TIME, headers, null, null);

    ByteArrayKafkaRecord byteArrayKafkaRecord = RowsWithMetadata.toExternalKafkaRecord(kafkaRecord);

    assertEquals("dummyTopic", byteArrayKafkaRecord.topic);
    assertEquals(111, byteArrayKafkaRecord.partition);
    assertEquals(222, byteArrayKafkaRecord.offset);
    assertEquals(12345, byteArrayKafkaRecord.timestamp);
    assertEquals(KafkaTimestampType.LOG_APPEND_TIME.id, byteArrayKafkaRecord.timestampTypeId);
    assertEquals(KafkaTimestampType.LOG_APPEND_TIME.name, byteArrayKafkaRecord.timestampTypeName);
    assertNull(byteArrayKafkaRecord.key);
    assertNull(byteArrayKafkaRecord.value);
    assertEquals(1, byteArrayKafkaRecord.headers.size());
    assertEquals("dummyHeaderKey", byteArrayKafkaRecord.headers.get(0).key);
    assertEquals("dummyHeaderVal", new String(byteArrayKafkaRecord.headers.get(0).value, "UTF-8"));
  }

  @Test
  public void testConstructKafkaReadWithoutMetadata() throws Exception {
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
                        Field.of("start_read_time", FieldType.INT64),
                        Field.of("commit_offset_in_finalize", FieldType.BOOLEAN),
                        Field.of("timestamp_policy", FieldType.STRING),
                        Field.of("redistribute_num_keys", FieldType.INT32),
                        Field.of("redistribute", FieldType.BOOLEAN),
                        Field.of("allow_duplicates", FieldType.BOOLEAN)))
                .withFieldValue("topics", topics)
                .withFieldValue("consumer_config", consumerConfig)
                .withFieldValue("key_deserializer", keyDeserializer)
                .withFieldValue("value_deserializer", valueDeserializer)
                .withFieldValue("start_read_time", startReadTime)
                .withFieldValue("commit_offset_in_finalize", false)
                .withFieldValue("timestamp_policy", "ProcessingTime")
                .withFieldValue("redistribute_num_keys", 0)
                .withFieldValue("redistribute", false)
                .withFieldValue("allow_duplicates", false)
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
                            .setUrn(
                                org.apache.beam.sdk.io.kafka.KafkaIO.Read.External
                                    .URN_WITHOUT_METADATA)
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
        Matchers.hasItem(MatchesPattern.matchesPattern(".*KafkaIO-Read.*")));
    assertThat(
        transform.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*Remove-Kafka-Metadata.*")));
    assertThat(transform.getInputsCount(), Matchers.is(0));
    assertThat(transform.getOutputsCount(), Matchers.is(1));

    RunnerApi.PTransform kafkaReadComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));
    result.getComponents().getTransformsOrThrow(kafkaReadComposite.getSubtransforms(0));

    verifyKafkaReadComposite(
        result.getComponents().getTransformsOrThrow(kafkaReadComposite.getSubtransforms(0)),
        result);
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
                            .setUrn(org.apache.beam.sdk.io.kafka.KafkaIO.Write.External.URN)
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
        Matchers.hasItem(MatchesPattern.matchesPattern(".*Kafka-ProducerRecord.*")));
    assertThat(
        transform.getSubtransformsList(),
        Matchers.hasItem(MatchesPattern.matchesPattern(".*KafkaIO-WriteRecords.*")));
    assertThat(transform.getInputsCount(), Matchers.is(1));
    assertThat(transform.getOutputsCount(), Matchers.is(0));

    RunnerApi.PTransform writeComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(1));
    RunnerApi.PTransform writeParDo =
        result.getComponents().getTransformsOrThrow(writeComposite.getSubtransforms(0));

    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.parseFrom(writeParDo.getSpec().getPayload());
    DoFn<?, ?> kafkaWriter = ParDoTranslation.getDoFn(parDoPayload);
    assertThat(kafkaWriter, Matchers.instanceOf(KafkaWriter.class));
    KafkaIO.WriteRecords<?, ?> spec = Whitebox.getInternalState(kafkaWriter, "spec");

    assertThat(spec.getProducerConfig(), Matchers.is(producerConfig));
    assertThat(spec.getTopic(), Matchers.is(topic));
    assertThat(spec.getKeySerializer().getName(), Matchers.is(keySerializer));
    assertThat(spec.getValueSerializer().getName(), Matchers.is(valueSerializer));
  }

  private static ExternalConfigurationPayload encodeRow(Row row) {
    ByteStringOutputStream outputStream = new ByteStringOutputStream();
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
