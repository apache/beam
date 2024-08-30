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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.io.kafka.KafkaIO.Write;
import org.apache.beam.sdk.io.kafka.KafkaIO.WriteRecords;
import org.apache.beam.sdk.io.kafka.upgrade.KafkaIOTranslation.KafkaIOReadWithMetadataTranslator;
import org.apache.beam.sdk.io.kafka.upgrade.KafkaIOTranslation.KafkaIOWriteTranslator;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.construction.TransformUpgrader;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for KafkaIOTranslation. */
@RunWith(JUnit4.class)
public class KafkaIOTranslationTest {

  // A mapping from Read transform builder methods to the corresponding schema fields in
  // KafkaIOTranslation.
  static final Map<String, String> READ_TRANSFORM_SCHEMA_MAPPING = new HashMap<>();

  static {
    READ_TRANSFORM_SCHEMA_MAPPING.put("getConsumerConfig", "consumer_config");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getTopics", "topics");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getTopicPartitions", "topic_partitions");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getTopicPattern", "topic_pattern");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getKeyCoder", "key_coder");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getValueCoder", "value_coder");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getConsumerFactoryFn", "consumer_factory_fn");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getWatermarkFn", "watermark_fn");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getMaxNumRecords", "max_num_records");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getMaxReadTime", "max_read_time");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getStartReadTime", "start_read_time");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getStopReadTime", "stop_read_time");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getRedistributeNumKeys", "redistribute_num_keys");
    READ_TRANSFORM_SCHEMA_MAPPING.put(
        "isCommitOffsetsInFinalizeEnabled", "is_commit_offset_finalize_enabled");
    READ_TRANSFORM_SCHEMA_MAPPING.put("isDynamicRead", "is_dynamic_read");
    READ_TRANSFORM_SCHEMA_MAPPING.put(
        "getWatchTopicPartitionDuration", "watch_topic_partition_duration");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getTimestampPolicyFactory", "timestamp_policy_factory");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getOffsetConsumerConfig", "offset_consumer_config");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getKeyDeserializerProvider", "key_deserializer_provider");
    READ_TRANSFORM_SCHEMA_MAPPING.put(
        "getValueDeserializerProvider", "value_deserializer_provider");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getCheckStopReadingFn", "check_stop_reading_fn");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getConsumerPollingTimeout", "consumer_polling_timeout");
  }

  // A mapping from Write transform builder methods to the corresponding schema fields in
  // KafkaIOTranslation.
  static final Map<String, String> WRITE_TRANSFORM_SCHEMA_MAPPING = new HashMap<>();

  static {
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getTopic", "topic");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getProducerConfig", "producer_config");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getProducerFactoryFn", "producer_factory_fn");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getKeySerializer", "key_serializer");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getValueSerializer", "value_serializer");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getPublishTimestampFunction", "publish_timestamp_fn");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("isEOS", "eos");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getSinkGroupId", "sink_group_id");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getNumShards", "num_shards");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getConsumerFactoryFn", "consumer_factory_fn");
  }

  @Test
  public void testReCreateReadTransformFromRow() throws Exception {
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("dummyconfig", "dummyvalue");

    Read<String, Integer> readTransform =
        KafkaIO.<String, Integer>read()
            .withBootstrapServers("dummykafkaserver")
            .withTopicPartitions(
                IntStream.range(0, 1)
                    .mapToObj(i -> new TopicPartition("dummytopic", i))
                    .collect(Collectors.toList()))
            .withConsumerConfigUpdates(consumerConfig);
    KafkaIOTranslation.KafkaIOReadWithMetadataTranslator translator =
        new KafkaIOReadWithMetadataTranslator();
    Row row = translator.toConfigRow(readTransform);

    Read<String, Integer> readTransformFromRow =
        (Read<String, Integer>) translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(
        readTransformFromRow.getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(
        "dummykafkaserver",
        readTransformFromRow.getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(1, readTransformFromRow.getTopicPartitions().size());
    assertEquals("dummytopic", readTransformFromRow.getTopicPartitions().get(0).topic());
    assertEquals(0, readTransformFromRow.getTopicPartitions().get(0).partition());
  }

  @Test
  public void testReCreateReadTransformWithTopics() throws Exception {
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("dummyconfig", "dummyvalue");

    Read<String, Integer> readTransform =
        KafkaIO.<String, Integer>read()
            .withBootstrapServers("dummykafkaserver")
            .withTopic("dummytopic")
            .withConsumerConfigUpdates(consumerConfig);
    KafkaIOTranslation.KafkaIOReadWithMetadataTranslator translator =
        new KafkaIOReadWithMetadataTranslator();
    Row row = translator.toConfigRow(readTransform);

    Read<String, Integer> readTransformFromRow =
        (Read<String, Integer>) translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(
        readTransformFromRow.getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(
        "dummykafkaserver",
        readTransformFromRow.getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(1, readTransformFromRow.getTopics().size());
    assertEquals("dummytopic", readTransformFromRow.getTopics().get(0));
  }

  @Test
  public void testReadTransformRowIncludesAllFields() throws Exception {
    // TODO: support 'withBadRecordErrorHandler' property.
    List<String> fieldsToIgnore =
        ImmutableList.of("getBadRecordRouter", "getBadRecordErrorHandler");
    List<String> getMethodNames =
        Arrays.stream(Read.class.getDeclaredMethods())
            .map(
                method -> {
                  return method.getName();
                })
            .filter(methodName -> methodName.startsWith("get"))
            .filter(methodName -> !fieldsToIgnore.contains(methodName))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'KafkaIO.Read' transform. Please update "
              + "'KafkaIOTranslation.KafkaIOReadWithMetadataTranslator' to track the new method "
              + "and update this test.",
          READ_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `readTransformMethodNameToSchemaFieldMapping` are
    // actually available in the schema.
    READ_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the read transform schema defined in "
                      + "KafkaIOReadWithMetadataTranslator.",
                  KafkaIOReadWithMetadataTranslator.schema.getFieldNames().contains(fieldName));
            });
  }

  @Test
  public void testReCreateWriteTransformFromRow() throws Exception {
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put("dummyconfig", "dummyvalue");
    Write<String, Integer> writeTransform =
        KafkaIO.<String, Integer>write()
            .withBootstrapServers("dummybootstrapserver")
            .withTopic("dummytopic")
            .withProducerConfigUpdates(producerConfig);
    KafkaIOTranslation.KafkaIOWriteTranslator translator =
        new KafkaIOTranslation.KafkaIOWriteTranslator();
    Row row = translator.toConfigRow(writeTransform);

    Write<String, Integer> writeTransformFromRow =
        (Write<String, Integer>) translator.fromConfigRow(row, PipelineOptionsFactory.create());
    WriteRecords<String, Integer> writeRecordsTransform =
        writeTransformFromRow.getWriteRecordsTransform();
    assertNotNull(
        writeRecordsTransform.getProducerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(
        "dummybootstrapserver",
        writeRecordsTransform.getProducerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("dummytopic", writeRecordsTransform.getTopic());
    Map<String, Object> producerConfigFromRow = writeRecordsTransform.getProducerConfig();
    assertTrue(producerConfigFromRow.containsKey("dummyconfig"));
    assertEquals("dummyvalue", producerConfigFromRow.get("dummyconfig"));
  }

  @Test
  public void testWriteTransformRowIncludesAllFields() throws Exception {
    // For these fields, default value will suffice (so no need to serialize when upgrading).
    // TODO: support 'withBadRecordErrorHandler' property.
    List<String> fieldsToIgnore =
        ImmutableList.of("getBadRecordRouter", "getBadRecordErrorHandler");

    // Write transform delegates property handling to the WriteRecords class. So we inspect the
    // WriteRecords class here.
    List<String> getMethodNames =
        Arrays.stream(WriteRecords.class.getDeclaredMethods())
            .map(
                method -> {
                  return method.getName();
                })
            .filter(methodName -> methodName.startsWith("get"))
            .filter(methodName -> !fieldsToIgnore.contains(methodName))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'KafkaIO.Write' transform. Please update "
              + "'KafkaIOTranslation.KafkaIOWriteTranslator' to track the new method and update "
              + "this test.",
          WRITE_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `writeTransformMethodNameToSchemaFieldMapping` are
    // actually available in the schema.
    WRITE_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the write transform schema defined in "
                      + "KafkaIOWriteWithMetadataTranslator.",
                  KafkaIOWriteTranslator.schema.getFieldNames().contains(fieldName));
            });
  }

  @Test
  public void testReadTransformURNDiscovery() {
    Read<String, Integer> readTransform =
        KafkaIO.<String, Integer>read()
            .withBootstrapServers("dummykafkaserver")
            .withTopicPartitions(
                IntStream.range(0, 1)
                    .mapToObj(i -> new TopicPartition("dummytopic", i))
                    .collect(Collectors.toList()));

    assertEquals(
        KafkaIOTranslation.KAFKA_READ_WITH_METADATA_TRANSFORM_URN_V2,
        TransformUpgrader.findUpgradeURN(readTransform));
  }

  @Test
  public void testWriteTransformURNDiscovery() {
    Write<String, Integer> writeTransform =
        KafkaIO.<String, Integer>write()
            .withBootstrapServers("dummybootstrapserver")
            .withTopic("dummytopic");

    assertEquals(
        KafkaIOTranslation.KAFKA_WRITE_TRANSFORM_URN_V2,
        TransformUpgrader.findUpgradeURN(writeTransform));
  }
}
