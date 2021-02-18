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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.beam.sdk.extensions.protobuf.PayloadMessages;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.thrift.payloads.SimpleThriftMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** UnitTest for {@link KafkaTableProvider}. */
public class KafkaTableProviderTest {
  private final KafkaTableProvider provider = new KafkaTableProvider();

  @Test
  public void testBuildBeamSqlCSVTable() {
    Table table = mockTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaCSVTable);

    BeamKafkaCSVTable csvTable = (BeamKafkaCSVTable) sqlTable;
    assertEquals("localhost:9092", csvTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), csvTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlAvroTable() {
    Table table = mockTable("hello", "avro");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaTable);

    BeamKafkaTable csvTable = (BeamKafkaTable) sqlTable;
    assertEquals("localhost:9092", csvTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), csvTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlProtoTable() {
    Table table = mockProtoTable("hello", PayloadMessages.SimpleMessage.class);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaTable);

    BeamKafkaTable protoTable = (BeamKafkaTable) sqlTable;
    assertEquals("localhost:9092", protoTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), protoTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlThriftTable() {
    Table table =
        mockThriftTable("hello", SimpleThriftMessage.class, TCompactProtocol.Factory.class);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaTable);

    BeamKafkaTable thriftTable = (BeamKafkaTable) sqlTable;
    assertEquals("localhost:9092", thriftTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), thriftTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlNestedBytesTable() {
    Table table = mockNestedBytesTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof NestedPayloadKafkaTable);

    BeamKafkaTable thriftTable = (BeamKafkaTable) sqlTable;
    assertEquals("localhost:9092", thriftTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), thriftTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlNestedThriftTable() {
    Table table =
        mockNestedThriftTable("hello", SimpleThriftMessage.class, TCompactProtocol.Factory.class);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof NestedPayloadKafkaTable);

    BeamKafkaTable thriftTable = (BeamKafkaTable) sqlTable;
    assertEquals("localhost:9092", thriftTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), thriftTable.getTopics());
  }

  @Test
  public void testGetTableType() {
    assertEquals("kafka", provider.getTableType());
  }

  private static Table mockTable(String name) {
    return mockTable(name, false, null, null, null, null);
  }

  private static Table mockTable(String name, String payloadFormat) {
    return mockTable(name, false, payloadFormat, null, null, null);
  }

  private static Table mockProtoTable(String name, Class<?> protoClass) {
    return mockTable(name, false, "proto", protoClass, null, null);
  }

  private static Table mockThriftTable(
      String name,
      Class<? extends TBase<?, ?>> thriftClass,
      Class<? extends TProtocolFactory> thriftProtocolFactoryClass) {
    return mockTable(name, false, "thrift", null, thriftClass, thriftProtocolFactoryClass);
  }

  private static Table mockNestedBytesTable(String name) {
    return mockTable(name, true, null, null, null, null);
  }

  private static Table mockNestedThriftTable(
      String name,
      Class<? extends TBase<?, ?>> thriftClass,
      Class<? extends TProtocolFactory> thriftProtocolFactoryClass) {
    return mockTable(name, true, "thrift", null, thriftClass, thriftProtocolFactoryClass);
  }

  private static Table mockTable(
      String name,
      boolean isNested,
      @Nullable String payloadFormat,
      @Nullable Class<?> protoClass,
      @Nullable Class<? extends TBase<?, ?>> thriftClass,
      @Nullable Class<? extends TProtocolFactory> thriftProtocolFactoryClass) {
    JSONObject properties = new JSONObject();
    properties.put("bootstrap.servers", "localhost:9092");
    JSONArray topics = new JSONArray();
    topics.add("topic1");
    topics.add("topic2");
    properties.put("topics", topics);
    if (payloadFormat != null) {
      properties.put("format", payloadFormat);
    }
    if (protoClass != null) {
      properties.put("protoClass", protoClass.getName());
    }
    if (thriftClass != null) {
      properties.put("thriftClass", thriftClass.getName());
    }
    if (thriftProtocolFactoryClass != null) {
      properties.put("thriftProtocolFactoryClass", thriftProtocolFactoryClass.getName());
    }
    Schema payloadSchema = Schema.builder().addInt32Field("id").addStringField("name").build();
    Schema schema;
    if (isNested) {
      Schema.Builder schemaBuilder = Schema.builder();
      schemaBuilder.addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE);
      if (payloadFormat == null) {
        schemaBuilder.addByteArrayField(Schemas.PAYLOAD_FIELD);
      } else {
        schemaBuilder.addRowField(Schemas.PAYLOAD_FIELD, payloadSchema);
      }
      schema = schemaBuilder.build();
    } else {
      schema = payloadSchema;
    }

    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("kafka://localhost:2181/brokers?topic=test")
        .schema(schema)
        .type("kafka")
        .properties(properties)
        .build();
  }
}
