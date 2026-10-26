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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.parser.impl.BeamSqlParserImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BeamBigQuerySqlDialect;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

/** UnitTest for {@link BeamSqlParserImpl}. */
public class BeamDDLTest {

  @Test
  public void testParseCreateExternalTable_full() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    ObjectNode properties = TableUtils.emptyProperties();
    ArrayNode hello = TableUtils.getObjectMapper().createArrayNode();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'test' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");

    assertEquals(
        mockTable("person", "test", "person table", properties),
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
            + " TYPE 'test'"
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
            + "TYPE 'test' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");
  }

  @Test
  public void testParseCreateExternalTable_withoutTableComment() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    ObjectNode properties = TableUtils.emptyProperties();
    ArrayNode hello = TableUtils.getObjectMapper().createArrayNode();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'test' \n"
            + "LOCATION '/home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'");
    assertEquals(
        mockTable("person", "test", null, properties), tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_withoutTblProperties() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    env.executeDdl(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name') \n"
            + "TYPE 'test' \n"
            + "COMMENT 'person table' \n"
            + "LOCATION '/home/admin/person'\n");
    assertEquals(
        mockTable("person", "test", "person table", TableUtils.emptyProperties()),
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
            + "TYPE 'test' \n"
            + "COMMENT 'person table' \n");

    assertEquals(
        mockTable("person", "test", "person table", TableUtils.emptyProperties(), null),
        tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_minimal() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);

    env.executeDdl("CREATE EXTERNAL TABLE person (id INT) TYPE test");

    assertEquals(
        Table.builder()
            .name("person")
            .type("test")
            .schema(
                Stream.of(Schema.Field.of("id", CalciteUtils.INTEGER).withNullable(true))
                    .collect(toSchema()))
            .properties(TableUtils.emptyProperties())
            .build(),
        tableProvider.getTables().get("person"));
  }

  @Test
  public void testParseCreateExternalTable_primaryKey() throws Exception {
    TestTableProvider tableProvider = new PrimaryKeyTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);
    String create =
        "CREATE EXTERNAL TABLE person (id INT, region VARCHAR, enforced VARCHAR,"
            + " PRIMARY KEY (region, id) NOT ENFORCED) TYPE test";

    env.executeDdl(create);

    Table table = tableProvider.getTables().get("person");
    assertEquals(Arrays.asList("region", "id"), table.getPrimaryKey());
    assertEquals(
        Stream.of(
                Schema.Field.of("id", CalciteUtils.INTEGER),
                Schema.Field.of("region", CalciteUtils.VARCHAR),
                Schema.Field.nullable("enforced", CalciteUtils.VARCHAR))
            .collect(toSchema()),
        table.getSchema());
    assertThat(
        SqlParser.create(
                create,
                SqlParser.config().withLex(Lex.JAVA).withParserFactory(BeamSqlParser.FACTORY))
            .parseStmt()
            .toSqlString(new SqlDialect(SqlDialect.EMPTY_CONTEXT))
            .getSql(),
        containsString(", PRIMARY KEY (region, id) NOT ENFORCED) TYPE test"));

    env.executeDdl(
        "CREATE EXTERNAL TABLE keyed (name VARCHAR,"
            + " id INT NOT NULL PRIMARY KEY NOT ENFORCED COMMENT 'key') TYPE test");
    Table keyed = tableProvider.getTables().get("keyed");
    assertEquals(Arrays.asList("id"), keyed.getPrimaryKey());
    assertEquals("key", keyed.getSchema().getField("id").getDescription());
  }

  @Test
  public void testInvalidPrimaryKey() {
    BeamSqlEnv env = BeamSqlEnv.inMemory(new PrimaryKeyTableProvider(), new TextTableProvider());
    env.executeDdl("CREATE EXTERNAL TABLE keyed (id INT PRIMARY KEY NOT ENFORCED) TYPE test");
    env.executeDdl("CREATE EXTERNAL TABLE unkeyed (id INT) TYPE test");
    env.executeDdl("CREATE EXTERNAL TABLE lines (id INT NOT NULL) TYPE text");
    String[][] ddlAndErrors = {
      {
        "CREATE EXTERNAL TABLE person (id INT PRIMARY KEY NOT ENFORCED) TYPE text",
        "Invalid use of 'PRIMARY KEY': Table 'person' of type 'text' does not support primary keys."
      },
      // Text tables don't support ALTER either, so this also checks the key is validated first.
      {
        "ALTER TABLE lines ADD PRIMARY KEY (id) NOT ENFORCED",
        "Invalid use of 'PRIMARY KEY': Table 'lines' of type 'text' does not support primary keys."
      },
      {
        "CREATE EXTERNAL TABLE person (id INT, PRIMARY KEY (id, name) NOT ENFORCED) TYPE test",
        "PRIMARY KEY column 'name' is not a column of table 'person'."
      },
      {
        "CREATE EXTERNAL TABLE person (id INT, PRIMARY KEY (id, id) NOT ENFORCED) TYPE test",
        "PRIMARY KEY column 'id' is listed more than once."
      },
      {
        "ALTER TABLE keyed ADD PRIMARY KEY (id) NOT ENFORCED",
        "Table 'keyed' already has primary key [id]. Drop it first with DROP PRIMARY KEY."
      },
      {"ALTER TABLE unkeyed DROP PRIMARY KEY", "Table 'unkeyed' has no primary key to drop."},
      {
        "ALTER TABLE unkeyed ADD PRIMARY KEY (id) NOT ENFORCED",
        "PRIMARY KEY column 'id' must already be NOT NULL."
      }
    };
    for (String[] ddlAndError : ddlAndErrors) {
      IllegalArgumentException e =
          assertThrows(
              ddlAndError[0], IllegalArgumentException.class, () -> env.executeDdl(ddlAndError[0]));
      assertEquals(ddlAndError[1], e.getMessage());
    }
    // A table has one key, declared only among CREATE EXTERNAL TABLE's own columns.
    for (String ddl :
        new String[] {
          "CREATE EXTERNAL TABLE person (r ROW(id INT PRIMARY KEY NOT ENFORCED)) TYPE test",
          "ALTER TABLE keyed ADD COLUMNS (k INT NOT NULL PRIMARY KEY NOT ENFORCED)"
        }) {
      assertThrows(ddl, ParseException.class, () -> env.executeDdl(ddl));
    }
    for (String columns :
        new String[] {
          "(id INT PRIMARY KEY NOT ENFORCED, name VARCHAR PRIMARY KEY NOT ENFORCED)",
          "(id INT PRIMARY KEY NOT ENFORCED, PRIMARY KEY (id) NOT ENFORCED)"
        }) {
      ParseException twoKeys =
          assertThrows(
              columns,
              ParseException.class,
              () -> env.executeDdl("CREATE EXTERNAL TABLE person " + columns + " TYPE test"));
      assertThat(
          columns,
          twoKeys.getCause().getMessage(),
          containsString("A table can have only one PRIMARY KEY."));
    }
    // Beam SQL never checks key uniqueness, so a key without NOT ENFORCED fails at PRIMARY.
    for (String ddl :
        new String[] {
          "CREATE EXTERNAL TABLE person (id INT, PRIMARY KEY (id) ENFORCED) TYPE test",
          "CREATE EXTERNAL TABLE person (id INT PRIMARY KEY COMMENT 'id') TYPE test",
          "ALTER TABLE keyed ADD PRIMARY KEY (id)"
        }) {
      SqlParseException enforced =
          (SqlParseException)
              assertThrows(ddl, ParseException.class, () -> env.executeDdl(ddl)).getCause();
      assertThat(
          ddl,
          enforced.getMessage(),
          containsString(
              "Beam SQL doesn't enforce primary keys, so PRIMARY KEY must be declared NOT"
                  + " ENFORCED."));
      assertEquals(ddl, ddl.indexOf("PRIMARY") + 1, enforced.getPos().getColumnNum());
    }
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
    env.executeDdl("CREATE EXTERNAL TABLE test.person (id INT) TYPE test");

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
            + "TYPE 'test' \n"
            + "COMMENT 'person table' \n");

    assertNotNull(tableProvider.getTables().get("person"));

    env.executeDdl("drop table person");
    assertNull(tableProvider.getTables().get("person"));
  }

  @Test
  public void unparseScalarFunction() {
    SqlIdentifier name = new SqlIdentifier("foo", SqlParserPos.ZERO);
    SqlNode jarPath = SqlLiteral.createCharString("path/to/udf.jar", SqlParserPos.ZERO);
    SqlCreateFunction createFunction =
        new SqlCreateFunction(SqlParserPos.ZERO, false, name, jarPath, false);
    SqlWriter sqlWriter = new SqlPrettyWriter(BeamBigQuerySqlDialect.DEFAULT);

    createFunction.unparse(sqlWriter, 0, 0);

    assertEquals(
        "CREATE FUNCTION foo USING JAR 'path/to/udf.jar'", sqlWriter.toSqlString().getSql());
  }

  @Test
  public void unparseAggregateFunction() {
    SqlIdentifier name = new SqlIdentifier("foo", SqlParserPos.ZERO);
    SqlNode jarPath = SqlLiteral.createCharString("path/to/udf.jar", SqlParserPos.ZERO);
    SqlCreateFunction createFunction =
        new SqlCreateFunction(SqlParserPos.ZERO, false, name, jarPath, true);
    SqlWriter sqlWriter = new SqlPrettyWriter(BeamBigQuerySqlDialect.DEFAULT);

    createFunction.unparse(sqlWriter, 0, 0);

    assertEquals(
        "CREATE AGGREGATE FUNCTION foo USING JAR 'path/to/udf.jar'",
        sqlWriter.toSqlString().getSql());
  }

  /** A {@link TestTableProvider} whose tables support primary keys. */
  private static class PrimaryKeyTableProvider extends TestTableProvider {
    @Override
    public boolean supportsPrimaryKey(Table table) {
      return true;
    }
  }

  private static Table mockTable(String name, String type, String comment, ObjectNode properties) {
    return mockTable(name, type, comment, properties, "/home/admin/" + name);
  }

  private static Table mockTable(
      String name, String type, String comment, ObjectNode properties, String location) {

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
