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
import java.util.List;
import org.apache.beam.sdk.extensions.protobuf.PayloadMessages;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.thrift.payloads.SimpleThriftMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** UnitTest for {@link KafkaTableProvider}. */
public class KafkaTableProviderTest {
  private final KafkaTableProvider provider = new KafkaTableProvider();
  private static final String LOCATION_BROKER = "104.126.7.88:7743";
  private static final String LOCATION_TOPIC = "topic1";

  @Test
  public void testBuildBeamSqlCSVTable() {
    Table table = mockTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaCSVTable);

    BeamKafkaCSVTable kafkaTable = (BeamKafkaCSVTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testBuildWithExtraServers() {
    Table table =
        mockTableWithExtraServers("hello", ImmutableList.of("localhost:1111", "localhost:2222"));
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaCSVTable);

    BeamKafkaCSVTable kafkaTable = (BeamKafkaCSVTable) sqlTable;
    assertEquals(
        LOCATION_BROKER + ",localhost:1111,localhost:2222", kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testBuildWithExtraTopics() {
    Table table = mockTableWithExtraTopics("hello", ImmutableList.of("topic2", "topic3"));
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaCSVTable);

    BeamKafkaCSVTable kafkaTable = (BeamKafkaCSVTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC, "topic2", "topic3"), kafkaTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlAvroTable() {
    Table table = mockTable("hello", "avro");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaTable);

    BeamKafkaTable kafkaTable = (BeamKafkaTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlProtoTable() {
    Table table = mockProtoTable("hello", PayloadMessages.SimpleMessage.class);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaTable);

    BeamKafkaTable kafkaTable = (BeamKafkaTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlThriftTable() {
    Table table =
        mockThriftTable("hello", SimpleThriftMessage.class, TCompactProtocol.Factory.class);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaTable);

    BeamKafkaTable kafkaTable = (BeamKafkaTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlNestedBytesTable() {
    Table table = mockNestedBytesTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof NestedPayloadKafkaTable);

    BeamKafkaTable kafkaTable = (BeamKafkaTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testBuildBeamSqlNestedThriftTable() {
    Table table =
        mockNestedThriftTable("hello", SimpleThriftMessage.class, TCompactProtocol.Factory.class);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof NestedPayloadKafkaTable);

    BeamKafkaTable kafkaTable = (BeamKafkaTable) sqlTable;
    assertEquals(LOCATION_BROKER, kafkaTable.getBootstrapServers());
    assertEquals(ImmutableList.of(LOCATION_TOPIC), kafkaTable.getTopics());
  }

  @Test
  public void testGetTableType() {
    assertEquals("kafka", provider.getTableType());
  }

  private static Table mockTable(String name) {
    return mockTable(name, false, null, null, null, null, null, null);
  }

  private static Table mockTableWithExtraServers(String name, List<String> extraBootstrapServers) {
    return mockTable(name, false, extraBootstrapServers, null, null, null, null, null);
  }

  private static Table mockTableWithExtraTopics(String name, List<String> extraTopics) {
    return mockTable(name, false, null, extraTopics, null, null, null, null);
  }

  private static Table mockTable(String name, String payloadFormat) {
    return mockTable(name, false, null, null, payloadFormat, null, null, null);
  }

  private static Table mockProtoTable(String name, Class<?> protoClass) {
    return mockTable(name, false, null, null, "proto", protoClass, null, null);
  }

  private static Table mockThriftTable(
      String name,
      Class<? extends TBase<?, ?>> thriftClass,
      Class<? extends TProtocolFactory> thriftProtocolFactoryClass) {
    return mockTable(
        name, false, null, null, "thrift", null, thriftClass, thriftProtocolFactoryClass);
  }

  private static Table mockNestedBytesTable(String name) {
    return mockTable(name, true, null, null, null, null, null, null);
  }

  private static Table mockNestedThriftTable(
      String name,
      Class<? extends TBase<?, ?>> thriftClass,
      Class<? extends TProtocolFactory> thriftProtocolFactoryClass) {
    return mockTable(
        name, true, null, null, "thrift", null, thriftClass, thriftProtocolFactoryClass);
  }

  private static Table mockTable(
      String name,
      boolean isNested,
      @Nullable List<String> extraBootstrapServers,
      @Nullable List<String> extraTopics,
      @Nullable String payloadFormat,
      @Nullable Class<?> protoClass,
      @Nullable Class<? extends TBase<?, ?>> thriftClass,
      @Nullable Class<? extends TProtocolFactory> thriftProtocolFactoryClass) {
    JSONObject properties = new JSONObject();

    if (extraBootstrapServers != null) {
      JSONArray bootstrapServers = new JSONArray();
      bootstrapServers.addAll(extraBootstrapServers);
      properties.put("bootstrap_servers", bootstrapServers);
    }
    if (extraTopics != null) {
      JSONArray topics = new JSONArray();
      topics.addAll(extraTopics);
      properties.put("topics", topics);
    }

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
        .location(LOCATION_BROKER + "/" + LOCATION_TOPIC)
        .schema(schema)
        .type("kafka")
        .properties(properties)
        .build();
  }
}
