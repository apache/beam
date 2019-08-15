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

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

/** CustomTableResolverTest. */
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
  public static class CustomResolutionTestTableProvider extends TestTableProvider
      implements CustomTableResolver {

    List<TableName> parsedTableNames = null;

    @Override
    public void registerKnownTableNames(List<TableName> tableNames) {
      parsedTableNames = tableNames;
    }

    @Override
    public TableProvider getSubProvider(String name) {
      // TODO: implement with trie

      // If 'name' matches a sub-schema/sub-provider we start tracking
      // the subsequent calls to getSubProvider().
      //
      // Simple table ids and final table lookup
      //
      // If there is no matching sub-schema then returning null from here indicates
      // that 'name' is either not part of this schema or it's a table, not a sub-schema,
      // this will be checked right after this in a getTable() call.
      //
      // Because this is a getSubProvider() call it means Calcite expects
      // the sub-schema/sub-provider to be returned, not a table,
      // so we only need to check against known compound table identifiers.
      // If 'name' acutally represents a simple identifier then it will be checked
      // in a 'getTable()' call later. Unless there's the same sub-provider name,
      // in which case it's a conflict and we will use the sub-schema and not assume it's a table.
      // Calcite does the same.
      //
      // Here we find if there are any parsed tables that start from 'name' that belong to this
      // table provider.
      // We then create a fake tracking provider that in a trie-manner collects
      // getSubProvider()/getTable() calls by checking whether there are known parsed table names
      // matching what Calcite asks us for.
      List<TableName> tablesToLookFor =
          parsedTableNames.stream()
              .filter(TableName::isCompound)
              .filter(tableName -> tableName.getPrefix().equals(name))
              .collect(toList());

      return tablesToLookFor.size() > 0 ? new TableNameTrackingProvider(1, tablesToLookFor) : null;
    }

    class TableNameTrackingProvider extends TestTableProvider {
      int schemaLevel;
      List<TableName> tableNames;

      TableNameTrackingProvider(int schemaLevel, List<TableName> tableNames) {
        this.schemaLevel = schemaLevel;
        this.tableNames = tableNames;
      }

      @Override
      public TableProvider getSubProvider(String name) {
        // Find if any of the parsed table names have 'name' as part
        // of their path at current index.
        //
        // If there are, return a new tracking provider for such tables and incremented index.
        //
        // If there are none, it means something weird has happened and returning null
        // will make Calcite try other schemas. Maybe things will work out.
        //
        // However since we originally register all parsed table names for the given schema
        // in this provider we should only receive a getSubProvider() call for something unknown
        // when it's a leaf path element, i.e. actual table name, which will be handled in
        // getTable() call.
        List<TableName> matchingTables =
            tableNames.stream()
                .filter(TableName::isCompound)
                .filter(tableName -> tableName.getPath().size() > schemaLevel)
                .filter(tableName -> tableName.getPath().get(schemaLevel).equals(name))
                .collect(toList());

        return matchingTables.size() > 0
            ? new TableNameTrackingProvider(schemaLevel + 1, matchingTables)
            : null;
      }

      @Nullable
      @Override
      public Table getTable(String name) {

        // This is called only after getSubProvider() returned null,
        // and since we are tracking the actual parsed table names, this should
        // be it, there should exist a parsed table that matches the 'name'.

        Optional<TableName> matchingTable =
            tableNames.stream()
                .filter(tableName -> tableName.getTableName().equals(name))
                .findFirst();

        TableName tableName =
            matchingTable.orElseThrow(
                () ->
                    new IllegalStateException(
                        "Unexpected table '"
                            + name
                            + "' requested. Current schema level is "
                            + schemaLevel
                            + ". Current known table names: "
                            + tableNames.toString()));
        // For test we register tables with underscore instead of dots, so here we lookup the tables
        // with those underscore
        String actualTableName =
            String.join("_", tableName.getPath()) + "_" + tableName.getTableName();
        return CustomResolutionTestTableProvider.this.getTable(actualTableName);
      }

      @Override
      public synchronized BeamSqlTable buildBeamSqlTable(Table table) {
        return CustomResolutionTestTableProvider.this.buildBeamSqlTable(table);
      }
    }
  }

  @Test
  public void testSimpleId() throws Exception {
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable", row(1, "one"), row(2, "two"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable", row(1, "one"), row(2, "two"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(1, "one"), row(2, "two"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
    TestTableProvider tableProvider = new CustomResolutionTestTableProvider();
    tableProvider.createTable(
        Table.builder().name("testtable_blah_foo_bar").schema(BASIC_SCHEMA).type("test").build());
    tableProvider.addRows("testtable_blah_foo_bar", row(3, "customer"), row(2, "nobody"));

    TestTableProvider tableProvider2 = new CustomResolutionTestTableProvider();
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
