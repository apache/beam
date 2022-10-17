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
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@AutoService(SchemaTransformProvider.class)
public class KafkaSchemaTransformReadProvider
    extends TypedSchemaTransformProvider<KafkaSchemaTransformReadConfiguration> {

  @Override
  protected Class<KafkaSchemaTransformReadConfiguration> configurationClass() {
    return KafkaSchemaTransformReadConfiguration.class;
  }

  @Override
  protected SchemaTransform from(KafkaSchemaTransformReadConfiguration configuration) {
    return new KafkaReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "kafka:read";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Lists.newArrayList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Lists.newArrayList("OUTPUT");
  }

  private static class KafkaReadSchemaTransform implements SchemaTransform {
    private final KafkaSchemaTransformReadConfiguration configuration;

    KafkaReadSchemaTransform(KafkaSchemaTransformReadConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      final String avroSchema = configuration.getAvroSchema();
      final Integer groupId = configuration.hashCode() % Integer.MAX_VALUE;
      final String autoOffsetReset =
          configuration.getAutoOffsetResetConfig() == null
              ? "latest"
              : configuration.getAutoOffsetResetConfig();
      if (avroSchema != null) {
        assert configuration.getConfluentSchemaRegistryUrl() == null
            : "To read from Kafka, a schema must be provided directly or though Confluent "
                + "Schema Registry, but not both.";
        final Schema beamSchema =
            AvroUtils.toBeamSchema(new org.apache.avro.Schema.Parser().parse(avroSchema));
        SerializableFunction<byte[], Row> valueMapper =
            AvroUtils.getAvroBytesToRowFunction(beamSchema);
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            KafkaIO.Read<byte[], byte[]> kafkaRead =
                KafkaIO.readBytes()
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(
                            ConsumerConfig.GROUP_ID_CONFIG,
                            "kafka-read-provider-" + groupId,
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                            true,
                            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                            100,
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                            autoOffsetReset))
                    .withTopic(configuration.getTopic())
                    .withBootstrapServers(configuration.getBootstrapServers());

            return PCollectionRowTuple.of(
                "OUTPUT",
                input
                    .getPipeline()
                    .apply(kafkaRead.withoutMetadata())
                    .apply(Values.create())
                    .apply(MapElements.into(TypeDescriptors.rows()).via(valueMapper))
                    .setRowSchema(beamSchema));
          }
        };
      } else {
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
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(
                            ConsumerConfig.GROUP_ID_CONFIG,
                            "kafka-read-provider-" + groupId,
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                            true,
                            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                            100,
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                            autoOffsetReset))
                    .withKeyDeserializer(ByteArrayDeserializer.class)
                    .withValueDeserializer(
                        ConfluentSchemaRegistryDeserializerProvider.of(
                            confluentSchemaRegUrl, confluentSchemaRegSubject));

            PCollection<GenericRecord> kafkaValues =
                input.getPipeline().apply(kafkaRead.withoutMetadata()).apply(Values.create());

            assert kafkaValues.getCoder().getClass() == AvroCoder.class;
            AvroCoder<GenericRecord> coder = (AvroCoder<GenericRecord>) kafkaValues.getCoder();
            kafkaValues = kafkaValues.setCoder(AvroUtils.schemaCoder(coder.getSchema()));
            return PCollectionRowTuple.of("OUTPUT", kafkaValues.apply(Convert.toRows()));
          }
        };
      }
    }
  };
}
