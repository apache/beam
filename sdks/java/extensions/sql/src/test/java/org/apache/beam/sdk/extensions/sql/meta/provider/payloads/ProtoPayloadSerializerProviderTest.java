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
package org.apache.beam.sdk.extensions.sql.meta.provider.payloads;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.net.URL;
import org.apache.beam.sdk.extensions.sql.meta.provider.kafka.KafkaMessages;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoPayloadSerializerProviderTest {
  private static final Schema SHUFFLED_SCHEMA =
      Schema.builder()
          .addStringField("f_string")
          .addInt32Field("f_int")
          .addArrayField("f_float_array", Schema.FieldType.FLOAT)
          .addDoubleField("f_double")
          .addInt64Field("f_long")
          .build();
  private static final Row ROW =
      Row.withSchema(SHUFFLED_SCHEMA)
          .withFieldValue("f_string", "string")
          .withFieldValue("f_int", 123)
          .withFieldValue("f_float_array", ImmutableList.of(8.0f))
          .withFieldValue("f_double", 9.0)
          .withFieldValue("f_long", 456L)
          .build();
  private static final KafkaMessages.TestMessage MESSAGE =
      KafkaMessages.TestMessage.newBuilder()
          .setFLong(456)
          .setFInt(123)
          .setFDouble(9.0)
          .setFString("string")
          .addFFloatArray(8.0f)
          .build();

  private final ProtoPayloadSerializerProvider provider = new ProtoPayloadSerializerProvider();

  private static final URL DESCRIPTOR_SET_URL =
      checkArgumentNotNull(
          checkArgumentNotNull(ProtoPayloadSerializerProviderTest.class.getClassLoader())
              .getResource("kafka_messages.proto.dsc"));

  @Test
  public void invalidArgs() {
    assertThrows(
        IllegalArgumentException.class,
        () -> provider.getSerializer(SHUFFLED_SCHEMA, ImmutableMap.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> provider.getSerializer(SHUFFLED_SCHEMA, ImmutableMap.of("protoClass", "")));
    assertThrows(
        ClassCastException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA, ImmutableMap.of("protoClass", ImmutableList.class.getName())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoClass",
                    KafkaMessages.TestMessage.class.getName(),
                    "protoMessageName",
                    "")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoClass",
                    KafkaMessages.TestMessage.class.getName(),
                    "protoDescriptorSetFile",
                    "")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of("protoDescriptorSetFile", DESCRIPTOR_SET_URL.toString())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoMessageName", KafkaMessages.TestMessage.getDescriptor().getName())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoMessageName",
                    "bad_name",
                    "protoDescriptorSetFile",
                    DESCRIPTOR_SET_URL.toString())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoMessageName",
                    KafkaMessages.TestMessage.getDescriptor().getName(),
                    "protoDescriptorSetFile",
                    "bad_url")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                Schema.builder()
                    .addStringField("f_NOTACTUALLYINMESSAGE")
                    .addInt32Field("f_int")
                    .addArrayField("f_float_array", FieldType.FLOAT)
                    .addDoubleField("f_double")
                    .addInt64Field("f_long")
                    .build(),
                ImmutableMap.of("protoClass", KafkaMessages.TestMessage.class.getName())));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                Schema.builder()
                    .addStringField("f_NOTACTUALLYINMESSAGE")
                    .addInt32Field("f_int")
                    .addArrayField("f_float_array", FieldType.FLOAT)
                    .addDoubleField("f_double")
                    .addInt64Field("f_long")
                    .build(),
                ImmutableMap.of(
                    "protoMessageName",
                    KafkaMessages.TestMessage.getDescriptor().getName(),
                    "protoDescriptorSetFile",
                    DESCRIPTOR_SET_URL.toString())));
  }

  @Test
  public void serializeClass() throws Exception {
    byte[] bytes =
        provider
            .getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of("protoClass", KafkaMessages.TestMessage.class.getName()))
            .serialize(ROW);
    KafkaMessages.TestMessage result = KafkaMessages.TestMessage.parseFrom(bytes);
    assertEquals(MESSAGE, result);
  }

  @Test
  public void deserializeClass() {
    Row row =
        provider
            .getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of("protoClass", KafkaMessages.TestMessage.class.getName()))
            .deserialize(MESSAGE.toByteArray());
    assertEquals(ROW, row);
  }

  @Test
  public void serializeDescriptor() throws Exception {
    byte[] bytes =
        provider
            .getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoMessageName",
                    KafkaMessages.TestMessage.getDescriptor().getName(),
                    "protoDescriptorSetFile",
                    DESCRIPTOR_SET_URL.toString()))
            .serialize(ROW);
    KafkaMessages.TestMessage result = KafkaMessages.TestMessage.parseFrom(bytes);
    assertEquals(MESSAGE, result);
  }

  @Test
  public void deserializeDescriptor() {
    Row row =
        provider
            .getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "protoMessageName",
                    KafkaMessages.TestMessage.getDescriptor().getName(),
                    "protoDescriptorSetFile",
                    DESCRIPTOR_SET_URL.toString()))
            .deserialize(MESSAGE.toByteArray());
    assertEquals(ROW, row);
  }
}
