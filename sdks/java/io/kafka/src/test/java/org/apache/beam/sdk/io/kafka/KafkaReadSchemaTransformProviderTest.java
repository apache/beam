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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KafkaReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class KafkaReadSchemaTransformProviderTest {
  private static final String AVRO_SCHEMA =
      "{\"type\":\"record\",\"namespace\":\"com.example\","
          + "\"name\":\"FullName\",\"fields\":[{\"name\":\"first\",\"type\":\"string\"},"
          + "{\"name\":\"last\",\"type\":\"string\"}]}";

  @Test
  public void testValidConfigurations() {
    assertThrows(
        AssertionError.class,
        () -> {
          KafkaReadSchemaTransformConfiguration.builder()
              .setFormat("UNUSUAL_FORMAT")
              .setTopic("a_valid_topic")
              .setBootstrapServers("a_valid_server")
              .build()
              .validate();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          KafkaReadSchemaTransformConfiguration.builder()
              .setFormat("UNUSUAL_FORMAT")
              // .setTopic("a_valid_topic")  // Topic is mandatory
              .setBootstrapServers("a_valid_server")
              .build()
              .validate();
        });

    assertThrows(
        IllegalStateException.class,
        () -> {
          KafkaReadSchemaTransformConfiguration.builder()
              .setFormat("UNUSUAL_FORMAT")
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
            .filter(provider -> provider.getClass() == KafkaReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider kafkaProvider = providers.get(0);
    assertEquals(kafkaProvider.outputCollectionNames(), Lists.newArrayList("output", "errors"));
    assertEquals(kafkaProvider.inputCollectionNames(), Lists.newArrayList());

    assertEquals(
        Sets.newHashSet(
            "bootstrapServers",
            "topic",
            "schema",
            "autoOffsetResetConfig",
            "consumerConfigUpdates",
            "format",
            "confluentSchemaRegistrySubject",
            "confluentSchemaRegistryUrl",
            "errorHandling",
            "fileDescriptorPath",
            "messageName"),
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
            .filter(provider -> provider.getClass() == KafkaReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    KafkaReadSchemaTransformProvider kafkaProvider =
        (KafkaReadSchemaTransformProvider) providers.get(0);
    kafkaProvider.from(
        KafkaReadSchemaTransformConfiguration.builder()
            .setTopic("anytopic")
            .setBootstrapServers("anybootstrap")
            .setSchema(AVRO_SCHEMA)
            .build());
  }

  @Test
  public void testBuildTransformWithJsonSchema() throws IOException {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KafkaReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    KafkaReadSchemaTransformProvider kafkaProvider =
        (KafkaReadSchemaTransformProvider) providers.get(0);
    kafkaProvider.from(
        KafkaReadSchemaTransformConfiguration.builder()
            .setTopic("anytopic")
            .setBootstrapServers("anybootstrap")
            .setFormat("JSON")
            .setSchema(
                new String(
                    ByteStreams.toByteArray(
                        Objects.requireNonNull(
                            getClass().getResourceAsStream("/json-schema/basic_json_schema.json"))),
                    StandardCharsets.UTF_8))
            .build());
  }

  @Test
  public void testBuildTransformWithRawFormat() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KafkaReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    KafkaReadSchemaTransformProvider kafkaProvider =
        (KafkaReadSchemaTransformProvider) providers.get(0);
    kafkaProvider.from(
        KafkaReadSchemaTransformConfiguration.builder()
            .setTopic("anytopic")
            .setBootstrapServers("anybootstrap")
            .setFormat("RAW")
            .build());
  }

  @Test
  public void testBuildTransformWithProtoFormat() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KafkaReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    KafkaReadSchemaTransformProvider kafkaProvider =
        (KafkaReadSchemaTransformProvider) providers.get(0);

    kafkaProvider.from(
        KafkaReadSchemaTransformConfiguration.builder()
            .setTopic("anytopic")
            .setBootstrapServers("anybootstrap")
            .setFormat("PROTO")
            .setMessageName("MyMessage")
            .setFileDescriptorPath(
                Objects.requireNonNull(
                        getClass().getResource("/proto_byte/file_descriptor/proto_byte_utils.pb"))
                    .getPath())
            .build());
  }

  @Test
  public void testBuildTransformWithProtoFormatWrongMessageName() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == KafkaReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    KafkaReadSchemaTransformProvider kafkaProvider =
        (KafkaReadSchemaTransformProvider) providers.get(0);

    assertThrows(
        NullPointerException.class,
        () ->
            kafkaProvider.from(
                KafkaReadSchemaTransformConfiguration.builder()
                    .setTopic("anytopic")
                    .setBootstrapServers("anybootstrap")
                    .setFormat("PROTO")
                    .setMessageName("MyOtherMessage")
                    .setFileDescriptorPath(
                        Objects.requireNonNull(
                                getClass()
                                    .getResource("/proto_byte/file_descriptor/proto_byte_utils.pb"))
                            .getPath())
                    .build()));
  }
}
