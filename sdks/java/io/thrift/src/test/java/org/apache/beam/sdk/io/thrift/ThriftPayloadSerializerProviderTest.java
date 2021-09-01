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
package org.apache.beam.sdk.io.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.io.thrift.payloads.TestThriftMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ThriftPayloadSerializerProviderTest {
  private static final Schema SHUFFLED_SCHEMA =
      Schema.builder()
          .addStringField("f_string")
          .addInt32Field("f_int")
          .addArrayField("f_double_array", Schema.FieldType.DOUBLE)
          .addDoubleField("f_double")
          .addInt64Field("f_long")
          .build();
  private static final Row ROW =
      Row.withSchema(SHUFFLED_SCHEMA)
          .withFieldValue("f_string", "string")
          .withFieldValue("f_int", 123)
          .withFieldValue("f_double_array", ImmutableList.of(8.0))
          .withFieldValue("f_double", 9.0)
          .withFieldValue("f_long", 456L)
          .build();
  private static final TestThriftMessage MESSAGE =
      new TestThriftMessage()
          .setFLong(456L)
          .setFInt(123)
          .setFDouble(9.0)
          .setFString("string")
          .setFDoubleArray(ImmutableList.of(8.0));

  private final PayloadSerializerProvider provider = new ThriftPayloadSerializerProvider();

  @Test
  public void invalidArgs() {
    assertThrows(
        IllegalArgumentException.class,
        () -> provider.getSerializer(SHUFFLED_SCHEMA, ImmutableMap.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of("thriftClass", "", "thriftProtocolFactoryClass", "")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "thriftClass",
                    "",
                    "thriftProtocolFactoryClass",
                    TCompactProtocol.Factory.class.getName())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "thriftClass",
                    TestThriftMessage.class.getName(),
                    "thriftProtocolFactoryClass",
                    "")));
    assertThrows(
        ClassCastException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "thriftClass", ImmutableList.class.getName(),
                    "thriftProtocolFactoryClass", TCompactProtocol.Factory.class.getName())));
    assertThrows(
        ClassCastException.class,
        () ->
            provider.getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "thriftClass", TestThriftMessage.class.getName(),
                    "thriftProtocolFactoryClass", ImmutableList.class.getName())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            provider.getSerializer(
                Schema.builder()
                    .addStringField("f_NOTACTUALLYINMESSAGE")
                    .addInt32Field("f_int")
                    .addArrayField("f_double_array", Schema.FieldType.DOUBLE)
                    .addDoubleField("f_double")
                    .addInt64Field("f_long")
                    .build(),
                ImmutableMap.of(
                    "thriftClass", TestThriftMessage.class.getName(),
                    "thriftProtocolFactoryClass", TCompactProtocol.Factory.class.getName())));
  }

  @Test
  public void serialize() throws Exception {
    byte[] bytes =
        provider
            .getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "thriftClass", TestThriftMessage.class.getName(),
                    "thriftProtocolFactoryClass", TCompactProtocol.Factory.class.getName()))
            .serialize(ROW);
    TestThriftMessage result = new TestThriftMessage();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(result, bytes);
    assertEquals(MESSAGE, result);
  }

  @Test
  public void deserialize() throws Exception {
    Row row =
        provider
            .getSerializer(
                SHUFFLED_SCHEMA,
                ImmutableMap.of(
                    "thriftClass", TestThriftMessage.class.getName(),
                    "thriftProtocolFactoryClass", TCompactProtocol.Factory.class.getName()))
            .deserialize(new TSerializer(new TCompactProtocol.Factory()).serialize(MESSAGE));
    assertEquals(ROW, row);
  }
}
