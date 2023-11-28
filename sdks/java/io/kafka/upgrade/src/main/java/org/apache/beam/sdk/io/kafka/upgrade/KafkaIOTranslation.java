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
package org.apache.beam.sdk.io.kafka.upgrade;

import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.io.kafka.DeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.io.kafka.KafkaIO.Write;
import org.apache.beam.sdk.io.kafka.KafkaIO.WriteRecords;
import org.apache.beam.sdk.io.kafka.KafkaIOUtils;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Utility methods for translating {@link KafkaIO} transforms to and from {@link RunnerApi}
 * representations.
 */
public class KafkaIOTranslation {

  // We define new v2 URNs here for KafkaIO transforms that includes all properties of Java
  // transforms. Kafka read/write v1 URNs are defined in KafkaIO.java and offer a limited set of
  // properties adjusted to cross-language usage with portable types.
  public static final String KAFKA_READ_WITH_METADATA_TRANSFORM_URN_V2 =
      "beam:transform:org.apache.beam:kafka_read_with_metadata:v2";
  public static final String KAFKA_WRITE_TRANSFORM_URN_V2 =
      "beam:transform:org.apache.beam:kafka_write:v2";

  private static byte[] toByteArray(Object object) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Object fromByteArray(byte[] bytes) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bis)) {
      return in.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static class KafkaIOReadWithMetadataTranslator implements TransformPayloadTranslator<Read<?, ?>> {

    static Schema topicPartitionSchema =
        Schema.builder().addStringField("topic").addInt32Field("partition").build();

    static Schema schema =
        Schema.builder()
            .addMapField("consumer_config", FieldType.STRING, FieldType.BYTES)
            .addNullableArrayField("topics", FieldType.STRING)
            .addNullableArrayField("topic_partitions", FieldType.row(topicPartitionSchema))
            .addNullableStringField("topic_pattern")
            .addNullableByteArrayField("key_coder")
            .addNullableByteArrayField("value_coder")
            .addByteArrayField("consumer_factory_fn")
            .addNullableByteArrayField("watermark_fn")
            .addInt64Field("max_num_records")
            .addNullableLogicalTypeField("max_read_time", new NanosDuration())
            .addNullableLogicalTypeField("start_read_time", new NanosInstant())
            .addNullableLogicalTypeField("stop_read_time", new NanosInstant())
            .addBooleanField("is_commit_offset_finalize_enabled")
            .addBooleanField("is_dynamic_read")
            .addNullableLogicalTypeField("watch_topic_partition_duration", new NanosDuration())
            .addByteArrayField("timestamp_policy_factory")
            .addNullableMapField("offset_consumer_config", FieldType.STRING, FieldType.BYTES)
            .addNullableByteArrayField("key_deserializer_provider")
            .addNullableByteArrayField("value_deserializer_provider")
            .addNullableByteArrayField("check_stop_reading_fn")
            .build();

    @Override
    public String getUrn() {
      return KAFKA_READ_WITH_METADATA_TRANSFORM_URN_V2;
    }

    @Override
    @SuppressWarnings({
      "type.argument",
    })
    public RunnerApi.@Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, Read<?, ?>> application, SdkComponents components)
        throws IOException {
      // Setting an empty payload since Kafka transform payload is not actually used by runners
      // currently.
      // This can be implemented if runners started actually using the Kafka transform payload.
      return FunctionSpec.newBuilder().setUrn(getUrn()).setPayload(ByteString.empty()).build();
    }

    @Override
    public Row toConfigRow(Read<?, ?> transform) {
      Map<String, Object> fieldValues = new HashMap<>();
      Map<String, byte[]> consumerConfigMap = new HashMap<>();
      transform
          .getConsumerConfig()
          .forEach(
              (key, val) -> {
                consumerConfigMap.put(key, toByteArray(val));
              });
      fieldValues.put("consumer_config", consumerConfigMap);
      if (transform.getTopics() != null) {
        fieldValues.put("topics", transform.getTopics());
      }

      if (transform.getTopicPartitions() != null) {
        List<Row> encodedTopicPartitions = new ArrayList<>();
        for (TopicPartition topicPartition : transform.getTopicPartitions()) {
          encodedTopicPartitions.add(
              Row.withSchema(topicPartitionSchema)
                  .addValue(topicPartition.topic())
                  .addValue(topicPartition.partition())
                  .build());
        }
        fieldValues.put("topic_partitions", encodedTopicPartitions);
      }
      if (transform.getTopicPattern() != null) {
        fieldValues.put("topic_pattern", transform.getTopicPattern().pattern());
      }
      if (transform.getKeyCoder() != null) {
        fieldValues.put("key_coder", toByteArray(transform.getKeyCoder()));
      }
      if (transform.getValueCoder() != null) {
        fieldValues.put("value_coder", toByteArray(transform.getValueCoder()));
      }
      if (transform.getConsumerFactoryFn() != null) {
        fieldValues.put("consumer_factory_fn", toByteArray(transform.getConsumerFactoryFn()));
      }
      if (transform.getWatermarkFn() != null) {
        fieldValues.put("watermark_fn", toByteArray(transform.getWatermarkFn()));
      }
      fieldValues.put("max_num_records", transform.getMaxNumRecords());
      if (transform.getMaxReadTime() != null) {
        fieldValues.put("max_read_time", transform.getMaxReadTime());
      }
      if (transform.getStartReadTime() != null) {
        fieldValues.put("start_read_time", transform.getStartReadTime());
      }
      if (transform.getStopReadTime() != null) {
        fieldValues.put("stop_read_time", transform.getStopReadTime());
      }

      fieldValues.put(
          "is_commit_offset_finalize_enabled", transform.isCommitOffsetsInFinalizeEnabled());
      fieldValues.put("is_dynamic_read", transform.isDynamicRead());
      if (transform.getWatchTopicPartitionDuration() != null) {
        fieldValues.put(
            "watch_topic_partition_duration", transform.getWatchTopicPartitionDuration());
      }

      fieldValues.put(
          "timestamp_policy_factory", toByteArray(transform.getTimestampPolicyFactory()));

      if (transform.getOffsetConsumerConfig() != null) {
        Map<String, byte[]> offsetConsumerConfigMap = new HashMap<>();
        transform
            .getOffsetConsumerConfig()
            .forEach(
                (key, val) -> {
                  offsetConsumerConfigMap.put(key, toByteArray(val));
                });
        fieldValues.put("offset_consumer_config", offsetConsumerConfigMap);
      }
      if (transform.getKeyDeserializerProvider() != null) {
        fieldValues.put(
            "key_deserializer_provider", toByteArray(transform.getKeyDeserializerProvider()));
      }
      if (transform.getValueDeserializerProvider() != null) {
        fieldValues.put(
            "value_deserializer_provider", toByteArray(transform.getValueDeserializerProvider()));
      }
      if (transform.getCheckStopReadingFn() != null) {
        fieldValues.put("check_stop_reading_fn", toByteArray(transform.getCheckStopReadingFn()));
      }

      return Row.withSchema(schema).withFieldValues(fieldValues).build();
    }

    @Override
    public Read<?, ?> fromConfigRow(Row configRow) {
      Read<?, ?> transform = KafkaIO.read();

      Map<String, byte[]> consumerConfig = configRow.getMap("consumer_config");
      if (consumerConfig != null) {
        Map<String, Object> updatedConsumerConfig = new HashMap<>();
        consumerConfig.forEach(
            (key, dataBytes) -> {
              // Adding all allowed properties.
              if (!KafkaIOUtils.DISALLOWED_CONSUMER_PROPERTIES.containsKey(key)) {
                if (consumerConfig.get(key) == null) {
                  throw new IllegalArgumentException(
                      "Encoded value of the consumer config property " + key + " was null");
                }
                updatedConsumerConfig.put(key, fromByteArray(consumerConfig.get(key)));
              }
            });
        transform = transform.withConsumerConfigUpdates(updatedConsumerConfig);
      }
      Collection<String> topics = configRow.getArray("topics");
      if (topics != null) {
        transform = transform.withTopics(new ArrayList<>(topics));
      }
      Collection<Row> topicPartitionRows = configRow.getArray("topic_partitions");
      if (topicPartitionRows != null) {
        Collection<TopicPartition> topicPartitions =
            topicPartitionRows.stream()
                .map(
                    row -> {
                      String topic = row.getString("topic");
                      if (topic == null) {
                        throw new IllegalArgumentException("Expected the topic to be not null");
                      }
                      Integer partition = row.getInt32("partition");
                      if (partition == null) {
                        throw new IllegalArgumentException("Expected the partition to be not null");
                      }
                      return new TopicPartition(topic, partition);
                    })
                .collect(Collectors.toList());
        transform = transform.withTopicPartitions(Lists.newArrayList(topicPartitions));
      }
      String topicPattern = configRow.getString("topic_pattern");
      if (topicPattern != null) {
        transform = transform.withTopicPattern(topicPattern);
      }

      byte[] keyDeserializerProvider = configRow.getBytes("key_deserializer_provider");
      if (keyDeserializerProvider != null) {

        byte[] keyCoder = configRow.getBytes("key_coder");
        if (keyCoder != null) {
          transform =
              transform.withKeyDeserializerProviderAndCoder(
                  (DeserializerProvider) fromByteArray(keyDeserializerProvider),
                  (org.apache.beam.sdk.coders.Coder) fromByteArray(keyCoder));
        } else {
          transform =
              transform.withKeyDeserializer(
                  (DeserializerProvider) fromByteArray(keyDeserializerProvider));
        }
      }

      byte[] valueDeserializerProvider = configRow.getBytes("value_deserializer_provider");
      if (valueDeserializerProvider != null) {
        byte[] valueCoder = configRow.getBytes("value_coder");
        if (valueCoder != null) {
          transform =
              transform.withValueDeserializerProviderAndCoder(
                  (DeserializerProvider) fromByteArray(valueDeserializerProvider),
                  (org.apache.beam.sdk.coders.Coder) fromByteArray(valueCoder));
        } else {
          transform =
              transform.withValueDeserializer(
                  (DeserializerProvider) fromByteArray(valueDeserializerProvider));
        }
      }

      byte[] consumerFactoryFn = configRow.getBytes("consumer_factory_fn");
      if (consumerFactoryFn != null) {
        transform =
            transform.withConsumerFactoryFn(
                (SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>)
                    fromByteArray(consumerFactoryFn));
      }
      byte[] watermarkFn = configRow.getBytes("watermark_fn");
      if (watermarkFn != null) {
        transform = transform.withWatermarkFn2((SerializableFunction) fromByteArray(watermarkFn));
      }
      Long maxNumRecords = configRow.getInt64("max_num_records");
      if (maxNumRecords != null) {
        transform = transform.withMaxNumRecords(maxNumRecords);
      }
      Duration maxReadTime = configRow.getValue("max_read_time");
      if (maxReadTime != null) {
        transform =
            transform.withMaxReadTime(org.joda.time.Duration.millis(maxReadTime.toMillis()));
      }
      Instant startReadTime = configRow.getValue("start_read_time");
      if (startReadTime != null) {
        transform = transform.withStartReadTime(startReadTime);
      }
      Instant stopReadTime = configRow.getValue("stop_read_time");
      if (stopReadTime != null) {
        transform = transform.withStopReadTime(stopReadTime);
      }
      Boolean isCommitOffsetFinalizeEnabled =
          configRow.getBoolean("is_commit_offset_finalize_enabled");
      if (isCommitOffsetFinalizeEnabled != null && isCommitOffsetFinalizeEnabled) {
        transform = transform.commitOffsetsInFinalize();
      }
      Boolean isDynamicRead = configRow.getBoolean("is_dynamic_read");
      if (isDynamicRead != null && isDynamicRead) {
        Duration watchTopicPartitionDuration = configRow.getValue("watch_topic_partition_duration");
        if (watchTopicPartitionDuration == null) {
          throw new IllegalArgumentException(
              "Expected watchTopicPartitionDuration to be available when isDynamicRead is set to true");
        }
        transform =
            transform.withDynamicRead(
                org.joda.time.Duration.millis(watchTopicPartitionDuration.toMillis()));
      }

      byte[] timestampPolicyFactory = configRow.getBytes("timestamp_policy_factory");
      if (timestampPolicyFactory != null) {
        transform =
            transform.withTimestampPolicyFactory(
                (TimestampPolicyFactory) fromByteArray(timestampPolicyFactory));
      }
      Map<String, byte[]> offsetConsumerConfig = configRow.getMap("offset_consumer_config");
      if (offsetConsumerConfig != null) {
        Map<String, Object> updatedOffsetConsumerConfig = new HashMap<>();
        offsetConsumerConfig.forEach(
            (key, dataBytes) -> {
              if (offsetConsumerConfig.get(key) == null) {
                throw new IllegalArgumentException(
                    "Encoded value for the offset consumer config key " + key + " was null.");
              }
              updatedOffsetConsumerConfig.put(key, fromByteArray(offsetConsumerConfig.get(key)));
            });
        transform = transform.withOffsetConsumerConfigOverrides(updatedOffsetConsumerConfig);
      }

      byte[] checkStopReadinfFn = configRow.getBytes("check_stop_reading_fn");
      if (checkStopReadinfFn != null) {
        transform =
            transform.withCheckStopReadingFn(
                (SerializableFunction<TopicPartition, Boolean>) fromByteArray(checkStopReadinfFn));
      }

      return transform;
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ReadRegistrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(Read.AUTOVALUE_CLASS, new KafkaIOReadWithMetadataTranslator())
          .build();
    }
  }

  static class KafkaIOWriteTranslator implements TransformPayloadTranslator<Write<?, ?>> {

    static Schema schema =
        Schema.builder()
            .addStringField("bootstrap_servers")
            .addNullableStringField("topic")
            .addNullableByteArrayField("key_serializer")
            .addNullableByteArrayField("value_serializer")
            .addNullableByteArrayField("producer_factory_fn")
            .addNullableByteArrayField("publish_timestamp_fn")
            .addBooleanField("eos")
            .addInt32Field("num_shards")
            .addNullableStringField("sink_group_id")
            .addNullableByteArrayField("consumer_factory_fn")
            .addNullableMapField("producer_config", FieldType.STRING, FieldType.BYTES)
            .build();

    @Override
    public String getUrn() {
      return KAFKA_WRITE_TRANSFORM_URN_V2;
    }

    @Override
    public String getUrn(Write<?, ?> transform) {
      return TransformPayloadTranslator.super.getUrn(transform);
    }

    @Override
    @SuppressWarnings({
      "type.argument",
    })
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, Write<?, ?>> application, SdkComponents components)
        throws IOException {
      {
        // Setting an empty payload since Kafka transform payload is not actually used by runners
        // currently.
        // This can be implemented if runners started actually using the Kafka transform payload.
        return FunctionSpec.newBuilder().setUrn(getUrn()).setPayload(ByteString.empty()).build();
      }
    }

    @Override
    public Row toConfigRow(Write<?, ?> transform) {
      Map<String, Object> fieldValues = new HashMap<>();

      WriteRecords<?, ?> writeRecordsTransform = transform.getWriteRecordsTransform();

      if (!writeRecordsTransform
          .getProducerConfig()
          .containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
        throw new IllegalArgumentException(
            "Expected the producer config to have 'ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG' set. Found: "
                + writeRecordsTransform.getProducerConfig());
      }
      fieldValues.put(
          "bootstrap_servers",
          writeRecordsTransform.getProducerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
      if (writeRecordsTransform.getTopic() != null) {
        fieldValues.put("topic", writeRecordsTransform.getTopic());
      }
      if (writeRecordsTransform.getKeySerializer() != null) {
        fieldValues.put("key_serializer", toByteArray(writeRecordsTransform.getKeySerializer()));
      }
      if (writeRecordsTransform.getValueSerializer() != null) {
        fieldValues.put(
            "value_serializer", toByteArray(writeRecordsTransform.getValueSerializer()));
      }
      if (writeRecordsTransform.getProducerFactoryFn() != null) {
        fieldValues.put(
            "producer_factory_fn", toByteArray(writeRecordsTransform.getProducerFactoryFn()));
      }
      if (writeRecordsTransform.getPublishTimestampFunction() != null) {
        fieldValues.put(
            "publish_timestamp_fn",
            toByteArray(writeRecordsTransform.getPublishTimestampFunction()));
      }

      fieldValues.put("eos", writeRecordsTransform.isEOS());
      fieldValues.put("num_shards", writeRecordsTransform.getNumShards());

      if (writeRecordsTransform.getSinkGroupId() != null) {
        fieldValues.put("sink_group_id", writeRecordsTransform.getSinkGroupId());
      }
      if (writeRecordsTransform.getConsumerFactoryFn() != null) {
        fieldValues.put(
            "consumer_factory_fn", toByteArray(writeRecordsTransform.getConsumerFactoryFn()));
      }

      if (writeRecordsTransform.getProducerConfig().size() > 0) {
        Map<String, byte[]> producerConfigMap = new HashMap<>();
        writeRecordsTransform
            .getProducerConfig()
            .forEach(
                (key, value) -> {
                  producerConfigMap.put((String) key, toByteArray(value));
                });
        fieldValues.put("producer_config", producerConfigMap);
      }

      return Row.withSchema(schema).withFieldValues(fieldValues).build();
    }

    @Override
    public Write<?, ?> fromConfigRow(Row configRow) {
      Write<?, ?> transform = KafkaIO.write();

      String bootstrapServers = configRow.getString("bootstrap_servers");
      if (bootstrapServers != null) {
        transform = transform.withBootstrapServers(bootstrapServers);
      }
      String topic = configRow.getValue("topic");
      if (topic != null) {
        transform = transform.withTopic(topic);
      }
      byte[] keySerializerBytes = configRow.getBytes("key_serializer");
      if (keySerializerBytes != null) {
        transform = transform.withKeySerializer((Class) fromByteArray(keySerializerBytes));
      }
      byte[] valueSerializerBytes = configRow.getBytes("value_serializer");
      if (valueSerializerBytes != null) {
        transform = transform.withValueSerializer((Class) fromByteArray(valueSerializerBytes));
      }
      byte[] producerFactoryFnBytes = configRow.getBytes("producer_factory_fn");
      if (producerFactoryFnBytes != null) {
        transform =
            transform.withProducerFactoryFn(
                (SerializableFunction) fromByteArray(producerFactoryFnBytes));
      }
      Boolean isEOS = configRow.getBoolean("eos");
      if (isEOS != null && isEOS) {
        Integer numShards = configRow.getInt32("num_shards");
        String sinkGroupId = configRow.getString("sink_group_id");
        if (numShards == null) {
          throw new IllegalArgumentException(
              "Expected numShards to be provided when EOS is set to true");
        }
        if (sinkGroupId == null) {
          throw new IllegalArgumentException(
              "Expected sinkGroupId to be provided when EOS is set to true");
        }
        transform = transform.withEOS(numShards, sinkGroupId);
      }
      byte[] consumerFactoryFnBytes = configRow.getBytes("consumer_factory_fn");
      if (consumerFactoryFnBytes != null) {
        transform =
            transform.withConsumerFactoryFn(
                (SerializableFunction) fromByteArray(consumerFactoryFnBytes));
      }

      Map<String, byte[]> producerConfig = configRow.getMap("producer_config");
      if (producerConfig != null && !producerConfig.isEmpty()) {
        Map<String, Object> updatedProducerConfig = new HashMap<>();
        producerConfig.forEach(
            (key, dataBytes) -> {
              updatedProducerConfig.put(key, fromByteArray((byte[]) dataBytes));
            });
        transform = transform.withProducerConfigUpdates(updatedProducerConfig);
      }

      return transform;
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class WriteRegistrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(Write.AUTOVALUE_CLASS, new KafkaIOWriteTranslator())
          .build();
    }
  }
}
