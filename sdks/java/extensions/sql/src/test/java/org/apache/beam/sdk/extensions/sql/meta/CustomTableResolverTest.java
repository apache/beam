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
package org.apache.beam.sdk.extensions.sql.meta;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.provider.FullNameTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

/** Test for custom table resolver and full name table provider. */
public class CustomTableResolverTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Schema BASIC_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();

  /**
   * Test table provider with custom name resolution.
   *
   * <p>Demonstrates how to parse table names as in normal Calcite queries syntax, e.g. {@code
   * a.b.c.d} and convert them to its' own custom table name format {@code a_b_c_d}.
   */
  public static class CustomResolutionTestTableProvider extends FullNameTableProvider {

    TestTableProvider delegateTableProvider;

    public CustomResolutionTestTableProvider() {
      delegateTableProvider = new TestTableProvider();
    }

    @Override
    public Table getTable(String tableName) {
      return delegateTableProvider.getTable(tableName);
    }

    @Override
    public Table getTableByFullName(TableName fullTableName) {
      // For the test we register tables with underscore instead of dots, so here we lookup the
      // tables
      // with those underscore.
      String actualTableName =
          String.join("_", fullTableName.getPath()) + "_" + fullTableName.getTableName();
      return delegateTableProvider.getTable(actualTableName);
    }

    @Override
    public String getTableType() {
      return delegateTableProvider.getTableType();
    }

    @Override
    public void createTable(Table table) {
      delegateTableProvider.createTable(table);
    }

    public void addRows(String tableName, Row... rows) {
      delegateTableProvider.addRows(tableName, rows);
    }

    @Override
    public void dropTable(String tableName) {
      delegateTableProvider.dropTable(tableName);
    }

    @Override
    public Map<String, Table> getTables() {
      return delegateTableProvider.getTables();
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
      return delegateTableProvider.buildBeamSqlTable(table);
    }
  }

  @Test
  public void testSimpleId() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable", row(1, "one"), row(2, "two"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testtable")
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testSimpleIdWithExplicitDefaultSchema() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable", row(1, "one"), row(2, "two"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider.testtable")
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testSimpleIdWithExplicitDefaultSchemaWithMultipleProviders() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable", row(1, "one"), row(2, "two"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable2").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable2", row(3, "three"), row(4, "four"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider2.testtable2")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(3, "three"), row(4, "four"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testSimpleIdWithExplicitNonDefaultSchema() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable", row(1, "one"), row(2, "two"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable2").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable2", row(3, "three"), row(4, "four"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider2.testtable2")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(3, "three"), row(4, "four"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testCompoundIdInDefaultSchema() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah", row(1, "one"), row(2, "two"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testtable.blah")
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testCompoundIdInExplicitDefaultSchema() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah", row(1, "one"), row(2, "two"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider.testtable.blah")
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testLongCompoundIdInDefaultSchema() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testtable.blah.foo.bar")
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testLongCompoundIdInDefaultSchemaWithMultipleProviders() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable_blah_foo_bar", row(3, "three"), row(4, "four"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testtable.blah.foo.bar")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testLongCompoundIdInExplicitDefaultSchema() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider.testtable.blah.foo.bar")
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "one"), row(2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testLongCompoundIdInNonDefaultSchemaSameTableNames() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable_blah_foo_bar", row(3, "three"), row(4, "four"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider2.testtable.blah.foo.bar")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(3, "three"), row(4, "four"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testLongCompoundIdInNonDefaultSchemaDifferentNames() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder()
            .name("testtable2_blah2_foo2_bar2")
            .schema(BASIC_SCHEMA)
            .type("test")
            .build());
    tableProvider2.addRows("testtable2_blah2_foo2_bar2", row(3, "three"), row(4, "four"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query("SELECT id, name FROM testprovider2.testtable2.blah2.foo2.bar2")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(3, "three"), row(4, "four"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testJoinWithLongCompoundIds() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable_blah_foo_bar2").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable_blah_foo_bar2", row(4, "customer"), row(1, "nobody"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query(
                    "SELECT testprovider2.testtable.blah.foo.bar2.id, testtable.blah.foo.bar.name \n"
                        + "FROM \n"
                        + "  testprovider2.testtable.blah.foo.bar2 \n"
                        + "JOIN \n"
                        + "  testtable.blah.foo.bar \n"
                        + "USING(name)")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(4, "customer"), row(1, "nobody"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testInnerJoinWithLongCompoundIds() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable_blah_foo_bar2").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable_blah_foo_bar2", row(4, "customer"), row(1, "nobody"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query(
                    "SELECT testprovider2.testtable.blah.foo.bar2.id, testtable.blah.foo.bar.name \n"
                        + "FROM \n"
                        + "  testprovider2.testtable.blah.foo.bar2 \n"
                        + "JOIN \n"
                        + "  testtable.blah.foo.bar \n"
                        + "USING(name)")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(4, "customer"), row(1, "nobody"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testJoinWithLongCompoundIdsWithAliases() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable_blah_foo_bar2").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable_blah_foo_bar2", row(4, "customer"), row(1, "nobody"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query(
                    "SELECT b.id, a.name \n"
                        + "FROM \n"
                        + "  testprovider2.testtable.blah.foo.bar2 AS b \n"
                        + "JOIN \n"
                        + "  testtable.blah.foo.bar a\n"
                        + "USING(name)")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result).containsInAnyOrder(row(4, "customer"), row(1, "nobody"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testUnionWithLongCompoundIds() throws Exception {
    CustomResolutionTestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    CustomResolutionTestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
    tableProvider2.createTable(
        Table.builder().name("testtable_blah_foo_bar2").schema(BASIC_SCHEMA).type("test").build());
    tableProvider2.addRows("testtable_blah_foo_bar2", row(4, "customer"), row(1, "nobody"));

    PCollection<Row> result =
        pipeline.apply(
            SqlTransform.query(
                    "SELECT id, name \n"
                        + "FROM \n"
                        + "  testprovider2.testtable.blah.foo.bar2 \n"
                        + "UNION \n"
                        + "    SELECT id, name \n"
                        + "      FROM \n"
                        + "        testtable.blah.foo.bar \n")
                .withTableProvider("testprovider2", tableProvider2)
                .withDefaultTableProvider("testprovider", tableProvider));

    PAssert.that(result)
        .containsInAnyOrder(
            row(4, "customer"), row(1, "nobody"), row(3, "customer"), row(2, "nobody"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  private Row row(int id, String name) {
    return Row.withSchema(BASIC_SCHEMA).addValues(id, name).build();
  }
}
