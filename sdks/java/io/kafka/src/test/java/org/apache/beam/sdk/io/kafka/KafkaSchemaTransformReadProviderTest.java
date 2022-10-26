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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KafkaSchemaTransformReadProvider}. */
@RunWith(JUnit4.class)
public class KafkaSchemaTransformReadProviderTest {
  private static final String AVRO_SCHEMA =
      "{\"type\":\"record\",\"namespace\":\"com.example\","
          + "\"name\":\"FullName\",\"fields\":[{\"name\":\"first\",\"type\":\"string\"},"
          + "{\"name\":\"last\",\"type\":\"string\"}]}";

  @Test
  public void testValidConfigurations() {
    assertThrows(
        AssertionError.class,
        () -> {
          KafkaSchemaTransformReadConfiguration.builder()
              .setDataFormat("UNUSUAL_FORMAT")
              .setTopic("a_valid_topic")
              .setBootstrapServers("a_valid_server")
              .build()
              .validate();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          KafkaSchemaTransformReadConfiguration.builder()
              .setDataFormat("UNUSUAL_FORMAT")
              // .setTopic("a_valid_topic")  // Topic is mandatory
              .setBootstrapServers("a_valid_server")
              .build()
              .validate();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          KafkaSchemaTransformReadConfiguration.builder()
              .setDataFormat("UNUSUAL_FORMAT")
              .setTopic("a_valid_topic")
              // .setBootstrapServers("a_valid_server")  // Bootstrap server is mandatory
              .build()
              .validate();
        });
  }

  @Test
  public void testFindTransformAndMakeItWork() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KafkaSchemaTransformReadProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider kafkaProvider = providers.get(0);
    assertEquals(kafkaProvider.outputCollectionNames(), Lists.newArrayList("OUTPUT"));
    assertEquals(kafkaProvider.inputCollectionNames(), Lists.newArrayList());

    assertEquals(
        Sets.newHashSet(
            "bootstrapServers",
            "topic",
            "avroSchema",
            "autoOffsetResetConfig",
            "consumerConfigUpdates",
            "dataFormat",
            "confluentSchemaRegistrySubject",
            "confluentSchemaRegistryUrl"),
        kafkaProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testBuildTransformWithAvroSchema() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KafkaSchemaTransformReadProvider.class)
            .collect(Collectors.toList());
    KafkaSchemaTransformReadProvider kafkaProvider =
        (KafkaSchemaTransformReadProvider) providers.get(0);
    kafkaProvider
        .from(
            KafkaSchemaTransformReadConfiguration.builder()
                .setTopic("anytopic")
                .setBootstrapServers("anybootstrap")
                .setAvroSchema(AVRO_SCHEMA)
                .build())
        .buildTransform();
  }
}
