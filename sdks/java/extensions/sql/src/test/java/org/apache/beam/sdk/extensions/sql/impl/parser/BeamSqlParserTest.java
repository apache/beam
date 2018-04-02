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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/**
 * UnitTest for {@link BeamSqlParser}.
 */
public class BeamSqlParserTest {
  @Test
  public void testParseCreateTable_full() throws Exception {
    JSONObject properties = new JSONObject();
    JSONArray hello = new JSONArray();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION 'text://home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'"
    );
    assertEquals(
        mockTable("person", "text", "person table", properties),
        table
    );
  }

  @Test(expected = org.apache.beam.sdk.extensions.sql.impl.parser.impl.ParseException.class)
  public void testParseCreateTable_withoutType() throws Exception {
    parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "COMMENT 'person table' \n"
            + "LOCATION 'text://home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'"
    );
  }

  @Test
  public void testParseCreateTable_withoutTableComment() throws Exception {
    JSONObject properties = new JSONObject();
    JSONArray hello = new JSONArray();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "LOCATION 'text://home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'"
    );
    assertEquals(mockTable("person", "text", null, properties), table);
  }

  @Test
  public void testParseCreateTable_withoutTblProperties() throws Exception {
    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION 'text://home/admin/person'\n"
    );
    assertEquals(
        mockTable("person", "text", "person table", new JSONObject()),
        table
    );
  }

  @Test
  public void testParseCreateTable_withoutLocation() throws Exception {
    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "TYPE 'text' \n"
            + "COMMENT 'person table' \n"
    );

    assertEquals(
        mockTable("person", "text", "person table", new JSONObject(), null),
        table
    );
  }

  @Test
  public void testParseDropTable() throws Exception {
    BeamSqlParser parser = new BeamSqlParser("drop table person");
    SqlNode sqlNode = parser.impl().parseSqlStmtEof();

    assertNotNull(sqlNode);
    assertTrue(sqlNode instanceof SqlDropTable);
    SqlDropTable stmt = (SqlDropTable) sqlNode;
    assertNotNull(stmt);
    assertEquals("person", stmt.tableName());
  }

  private Table parseTable(String sql) throws Exception {
    BeamSqlParser parser = new BeamSqlParser(sql);
    SqlNode sqlNode = parser.impl().parseSqlStmtEof();

    assertNotNull(sqlNode);
    assertTrue(sqlNode instanceof SqlCreateTable);
    SqlCreateTable stmt = (SqlCreateTable) sqlNode;
    return ParserUtils.convertCreateTableStmtToTable(stmt);
  }

  private static Table mockTable(String name, String type, String comment, JSONObject properties) {
    return mockTable(name, type, comment, properties, "text://home/admin/" + name);
  }

  private static Table mockTable(String name, String type, String comment, JSONObject properties,
      String location) {
    URI locationURI = null;
    if (location != null) {
      locationURI = URI.create(location);
    }

    return Table.builder()
        .name(name)
        .type(type)
        .comment(comment)
        .location(locationURI)
        .columns(ImmutableList.of(
            Column.builder()
                .name("id")
                .fieldType(TypeName.INT32.type())
                .primaryKey(false)
                .comment("id")
                .build(),
            Column.builder()
                .name("name")
                .fieldType(RowSqlTypes.VARCHAR)
                .primaryKey(false)
                .comment("name")
                .build()
        ))
        .properties(properties)
        .build();
  }
}
