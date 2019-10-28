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

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.junit.Test;

/** UnitTest for {@link KafkaTableProvider}. */
public class KafkaTableProviderTest {
  private KafkaTableProvider provider = new KafkaTableProvider();

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    Table table = mockTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaCSVTable);

    BeamKafkaCSVTable csvTable = (BeamKafkaCSVTable) sqlTable;
    assertEquals("localhost:9092", csvTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), csvTable.getTopics());
  }

  @Test
  public void testGetTableType() throws Exception {
    assertEquals("kafka", provider.getTableType());
  }

  private static Table mockTable(String name) {
    JSONObject properties = new JSONObject();
    properties.put("bootstrap.servers", "localhost:9092");
    JSONArray topics = new JSONArray();
    topics.add("topic1");
    topics.add("topic2");
    properties.put("topics", topics);

    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("kafka://localhost:2181/brokers?topic=test")
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("kafka")
        .properties(properties)
        .build();
  }
}
