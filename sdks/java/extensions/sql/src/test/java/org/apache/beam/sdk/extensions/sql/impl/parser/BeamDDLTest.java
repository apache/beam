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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

/** UnitTest for {@link BeamSqlParserImpl}. */
public class BeamDDLTest {

  @Test
  public void testParseCreateExternalTable_full() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    JSONObject properties = new JSONObject();
    JSONArray hello = new JSONArray();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");

    assertEquals(
        mockTable("person", "text", "person table", properties),
        tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_WithComplexFields() {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    env.executeDdl(
        "CREATE EXTERNAL TABLE PersonDetails"
            + " ( personInfo MAP<VARCHAR, ROW<field_1 INTEGER,field_2 VARCHAR>> , "
            + " additionalInfo ROW<field_0 TIMESTAMP,field_1 INTEGER,field_2 TINYINT> )"
            + " TYPE 'text'"
            + " LOCATION '/home/admin/person'");

    assertNotNull(tableProvider.getTables().get("PersonDetails"));
  }

  @Test(expected = ParseException.class)
  public void testParseCreateExternalTable_withoutType() throws Exception {
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(new TestTableProvider());
    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");
  }

  @Test(expected = ParseException.class)
  public void testParseCreateTable() throws Exception {
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(new TestTableProvider());
    env.executeDdl(
        "CREATE TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");
  }

  @Test
  public void testParseCreateExternalTable_withoutTableComment() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    JSONObject properties = new JSONObject();
    JSONArray hello = new JSONArray();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");
    assertEquals(
        mockTable("person", "text", null, properties), tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_withoutTblProperties() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n");
    assertEquals(
        mockTable("person", "text", "person table", new JSONObject()),
        tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_withoutLocation() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n");

    assertEquals(
        mockTable("person", "text", "person table", new JSONObject(), null),
        tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_minimal() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    env.executeDdl("CREATE EXTERNAL TABLE person (id INT) TYPE text");

    assertEquals(
        Table.builder()
            .name("person")
            .type("text")
            .schema(
                Stream.of(Schema.Field.of("id", CalciteUtils.INTEGER).withNullable(true))
                    .collect(toSchema()))
            .properties(new JSONObject())
            .build(),
        tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_withDatabase() throws Exception {
    TestTableProvider rootProvider = new TestTableProvider();
    TestTableProvider testProvider = new TestTableProvider();

    BeamSqlEnv env =
        BeamSqlEnv.builder(rootProvider)
            .addSchema("test", testProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
    assertNull(testProvider.getTables().get("person"));
    env.executeDdl("CREATE EXTERNAL TABLE test.person (id INT) TYPE text");

    assertNotNull(testProvider.getTables().get("person"));
  }

  @Test
  public void testParseDropTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    assertNull(tableProvider.getTables().get("person"));
    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n");

    assertNotNull(tableProvider.getTables().get("person"));

    env.executeDdl("drop table person");
    assertNull(tableProvider.getTables().get("person"));
  }

  private static Table mockTable(String name, String type, String comment, JSONObject properties) {
    return mockTable(name, type, comment, properties, "/home/admin/" + name);
  }

  private static Table mockTable(
      String name, String type, String comment, JSONObject properties, String location) {

    return Table.builder()
        .name(name)
        .type(type)
        .comment(comment)
        .location(location)
        .schema(
            Stream.of(
                    Schema.Field.of("id", CalciteUtils.INTEGER)
                        .withNullable(true)
                        .withDescription("id"),
                    Schema.Field.of("name", CalciteUtils.VARCHAR)
                        .withNullable(true)
                        .withDescription("name"))
                .collect(toSchema()))
        .properties(properties)
        .build();
  }
}
