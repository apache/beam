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

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;

@AutoService(SchemaTransformProvider.class)
public class KafkaReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<KafkaReadSchemaTransformConfiguration> {

  final Boolean isTest;
  final Integer testTimeoutSecs;

  public KafkaReadSchemaTransformProvider() {
    this(false, 0);
  }

  @VisibleForTesting
  KafkaReadSchemaTransformProvider(Boolean isTest, Integer testTimeoutSecs) {
    this.isTest = isTest;
    this.testTimeoutSecs = testTimeoutSecs;
  }

  @Override
  protected Class<KafkaReadSchemaTransformConfiguration> configurationClass() {
    return KafkaReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(KafkaReadSchemaTransformConfiguration configuration) {
    return new KafkaReadSchemaTransform(configuration, isTest, testTimeoutSecs);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:kafka_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Lists.newArrayList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Lists.newArrayList("output");
  }

  private static class KafkaReadSchemaTransform implements SchemaTransform {
    private final KafkaReadSchemaTransformConfiguration configuration;
    private final Boolean isTest;
    private final Integer testTimeoutSeconds;

    KafkaReadSchemaTransform(
        KafkaReadSchemaTransformConfiguration configuration,
        Boolean isTest,
        Integer testTimeoutSeconds) {
      configuration.validate();
      this.configuration = configuration;
      this.isTest = isTest;
      this.testTimeoutSeconds = testTimeoutSeconds;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      final String inputSchema = configuration.getSchema();
      final Integer groupId = configuration.hashCode() % Integer.MAX_VALUE;
      final String autoOffsetReset =
          MoreObjects.firstNonNull(configuration.getAutoOffsetResetConfig(), "latest");

      Map<String, Object> consumerConfigs =
          new HashMap<>(
              MoreObjects.firstNonNull(configuration.getConsumerConfigUpdates(), new HashMap<>()));
      consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-read-provider-" + groupId);
      consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
      consumerConfigs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
      consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

      if (inputSchema != null && !inputSchema.isEmpty()) {
        assert Strings.isNullOrEmpty(configuration.getConfluentSchemaRegistryUrl())
            : "To read from Kafka, a schema must be provided directly or though Confluent "
                + "Schema Registry, but not both.";
        final Schema beamSchema =
            Objects.equals(configuration.getFormat(), "JSON")
                ? JsonUtils.beamSchemaFromJsonSchema(inputSchema)
                : AvroUtils.toBeamSchema(new org.apache.avro.Schema.Parser().parse(inputSchema));
        SerializableFunction<byte[], Row> valueMapper =
            Objects.equals(configuration.getFormat(), "JSON")
                ? JsonUtils.getJsonBytesToRowFunction(beamSchema)
                : AvroUtils.getAvroBytesToRowFunction(beamSchema);
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            KafkaIO.Read<byte[], byte[]> kafkaRead =
                KafkaIO.readBytes()
                    .withConsumerConfigUpdates(consumerConfigs)
                    .withTopic(configuration.getTopic())
                    .withBootstrapServers(configuration.getBootstrapServers());
            if (isTest) {
              kafkaRead = kafkaRead.withMaxReadTime(Duration.standardSeconds(testTimeoutSeconds));
            }

            return PCollectionRowTuple.of(
                "output",
                input
                    .getPipeline()
                    .apply(kafkaRead.withoutMetadata())
                    .apply(Values.create())
                    .apply(MapElements.into(TypeDescriptors.rows()).via(valueMapper))
                    .setRowSchema(beamSchema));
          }
        };
      } else {
        assert !Strings.isNullOrEmpty(configuration.getConfluentSchemaRegistryUrl())
            : "To read from Kafka, a schema must be provided directly or though Confluent "
                + "Schema Registry. Neither seems to have been provided.";
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            final String confluentSchemaRegUrl = configuration.getConfluentSchemaRegistryUrl();
            final String confluentSchemaRegSubject =
                configuration.getConfluentSchemaRegistrySubject();
            if (confluentSchemaRegUrl == null || confluentSchemaRegSubject == null) {
              throw new IllegalArgumentException(
                  "To read from Kafka, a schema must be provided directly or though Confluent "
                      + "Schema Registry. Make sure you are providing one of these parameters.");
            }
            KafkaIO.Read<byte[], GenericRecord> kafkaRead =
                KafkaIO.<byte[], GenericRecord>read()
                    .withTopic(configuration.getTopic())
                    .withBootstrapServers(configuration.getBootstrapServers())
                    .withConsumerConfigUpdates(consumerConfigs)
                    .withKeyDeserializer(ByteArrayDeserializer.class)
                    .withValueDeserializer(
                        ConfluentSchemaRegistryDeserializerProvider.of(
                            confluentSchemaRegUrl, confluentSchemaRegSubject));
            if (isTest) {
              kafkaRead = kafkaRead.withMaxReadTime(Duration.standardSeconds(testTimeoutSeconds));
            }

            PCollection<GenericRecord> kafkaValues =
                input.getPipeline().apply(kafkaRead.withoutMetadata()).apply(Values.create());

            assert kafkaValues.getCoder().getClass() == AvroCoder.class;
            AvroCoder<GenericRecord> coder = (AvroCoder<GenericRecord>) kafkaValues.getCoder();
            kafkaValues = kafkaValues.setCoder(AvroUtils.schemaCoder(coder.getSchema()));
            return PCollectionRowTuple.of("output", kafkaValues.apply(Convert.toRows()));
          }
        };
      }
    }
  };
}
