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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.schemas.Schema.FieldType.array;
import static org.apache.beam.sdk.schemas.Schema.FieldType.row;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for writing to Iceberg with Beam SQL. */
@RunWith(JUnit4.class)
public class IcebergReadWriteIT {
  private static final Schema NESTED_SCHEMA =
      Schema.builder()
          .addNullableArrayField("c_arr_struct_arr", STRING)
          .addNullableInt32Field("c_arr_struct_integer")
          .build();
  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("c_bigint", INT64)
          .addNullableField("c_integer", INT32)
          .addNullableField("c_float", FLOAT)
          .addNullableField("c_double", DOUBLE)
          .addNullableField("c_boolean", BOOLEAN)
          .addNullableField("c_timestamp", CalciteUtils.TIMESTAMP)
          .addNullableField("c_varchar", STRING)
          .addNullableField("c_char", STRING)
          .addNullableField("c_arr", array(STRING))
          .addNullableField("c_arr_struct", array(row(NESTED_SCHEMA)))
          .build();

  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public TestName testName = new TestName();

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("IcebergReadWriteIT");
  private static final String BQMS_CATALOG =
      "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog";
  static final String DATASET = "iceberg_sql_tests_" + System.nanoTime();
  static String warehouse;
  protected static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);

  @BeforeClass
  public static void createDataset() throws IOException, InterruptedException {
    warehouse =
        format(
            "%s%s/%s",
            TestPipeline.testingPipelineOptions().getTempLocation(),
            IcebergReadWriteIT.class.getSimpleName(),
            UUID.randomUUID());
    BQ_CLIENT.createNewDataset(OPTIONS.getProject(), DATASET);
  }

  @AfterClass
  public static void deleteDataset() {
    BQ_CLIENT.deleteDataset(OPTIONS.getProject(), DATASET);
  }

  @Test
  public void testSqlWriteAndRead() throws IOException, InterruptedException {
    runSqlWriteAndRead(false);
  }

  @Test
  public void testSqlWriteWithPartitionFieldsAndRead() throws IOException, InterruptedException {
    runSqlWriteAndRead(true);
  }

  public void runSqlWriteAndRead(boolean withPartitionFields)
      throws IOException, InterruptedException {
    CatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlEnv sqlEnv =
        BeamSqlEnv.builder(catalogManager)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
    String tableIdentifier = DATASET + "." + testName.getMethodName();

    // 1) create Iceberg catalog
    String createCatalog =
        "CREATE CATALOG my_catalog \n"
            + "TYPE iceberg \n"
            + "PROPERTIES (\n"
            + format("  'catalog-impl' = '%s', \n", BQMS_CATALOG)
            + "  'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO', \n"
            + format("  'warehouse' = '%s', \n", warehouse)
            + format("  'gcp_project' = '%s', \n", OPTIONS.getProject())
            + "  'gcp_region' = 'us-central1')";
    sqlEnv.executeDdl(createCatalog);

    // 2) use the catalog we just created
    String setCatalog = "USE CATALOG my_catalog";
    sqlEnv.executeDdl(setCatalog);

    // 3) create beam table
    String partitionFields =
        withPartitionFields
            ? "PARTITIONED BY ('bucket(c_integer, 5)', 'c_boolean', 'hour(c_timestamp)', 'truncate(c_varchar, 3)') \n"
            : "";
    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR>, \n"
            + "   c_arr_struct ARRAY<ROW<c_arr_struct_arr ARRAY<VARCHAR>, c_arr_struct_integer INTEGER>> \n"
            + ") \n"
            + "TYPE 'iceberg' \n"
            + partitionFields
            + "LOCATION '"
            + tableIdentifier
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    // 3) verify a real Iceberg table was created, with the right partition spec
    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    IcebergTableProvider provider =
        (IcebergTableProvider) catalog.metaStore().getProvider("iceberg");
    Catalog icebergCatalog = provider.catalogConfig.catalog();
    PartitionSpec expectedSpec = PartitionSpec.unpartitioned();
    if (withPartitionFields) {
      expectedSpec =
          PartitionSpec.builderFor(IcebergUtils.beamSchemaToIcebergSchema(SOURCE_SCHEMA))
              .bucket("c_integer", 5)
              .identity("c_boolean")
              .hour("c_timestamp")
              .truncate("c_varchar", 3)
              .build();
    }
    Table icebergTable = icebergCatalog.loadTable(TableIdentifier.parse(tableIdentifier));
    assertEquals(expectedSpec, icebergTable.spec());
    assertEquals("my_catalog." + tableIdentifier, icebergTable.name());
    assertTrue(icebergTable.location().startsWith(warehouse));
    assertEquals(expectedSpec, icebergTable.spec());
    Schema expectedSchema = checkStateNotNull(provider.getTable("TEST")).getSchema();
    assertEquals(expectedSchema, IcebergUtils.icebergSchemaToBeamSchema(icebergTable.schema()));

    // 4) write to underlying Iceberg table
    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456'], "
            + "ARRAY["
            + "CAST(ROW(ARRAY['abc', 'xyz'], 123) AS ROW(c_arr_struct_arr VARCHAR ARRAY, c_arr_struct_integer INTEGER)), "
            + "CAST(ROW(ARRAY['foo', 'bar'], 456) AS ROW(c_arr_struct_arr VARCHAR ARRAY, c_arr_struct_integer INTEGER)), "
            + "CAST(ROW(ARRAY['cat', 'dog'], 789) AS ROW(c_arr_struct_arr VARCHAR ARRAY, c_arr_struct_integer INTEGER))]"
            + ")";
    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(insertStatement));
    writePipeline.run().waitUntilFinish();

    // 5) run external query on Iceberg table (hosted on BQ) to verify correct row was written
    String query = format("SELECT * FROM `%s.%s`", OPTIONS.getProject(), tableIdentifier);
    TableRow returnedRow =
        BQ_CLIENT.queryUnflattened(query, OPTIONS.getProject(), true, true).get(0);
    Row beamRow = BigQueryUtils.toBeamRow(SOURCE_SCHEMA, returnedRow);
    Row expectedRow =
        Row.withSchema(SOURCE_SCHEMA)
            .addValues(
                9223372036854775807L,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                asList("123", "456"),
                asList(
                    nestedRow(asList("abc", "xyz"), 123),
                    nestedRow(asList("foo", "bar"), 456),
                    nestedRow(asList("cat", "dog"), 789)))
            .build();
    assertEquals(expectedRow, beamRow);

    // 6) read using Beam SQL and verify
    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));
    PAssert.that(output).containsInAnyOrder(expectedRow);
    PipelineResult.State state = readPipeline.run().waitUntilFinish();
    assertThat(state, equalTo(PipelineResult.State.DONE));

    // 7) cleanup
    sqlEnv.executeDdl("DROP TABLE TEST");
    assertFalse(icebergCatalog.tableExists(TableIdentifier.parse(tableIdentifier)));
  }

  @Test
  public void testSQLReadWithProjectAndFilterPushDown() {
    BeamSqlEnv sqlEnv =
        BeamSqlEnv.builder(new InMemoryCatalogManager())
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
    String tableIdentifier = DATASET + "." + testName.getMethodName();

    // 1) create Iceberg catalog
    String createCatalog =
        "CREATE CATALOG my_catalog \n"
            + "TYPE iceberg \n"
            + "PROPERTIES (\n"
            + "  'catalog-impl' = 'org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog', \n"
            + "  'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO', \n"
            + format("  'warehouse' = '%s', \n", warehouse)
            + format("  'gcp_project' = '%s', \n", OPTIONS.getProject())
            + "  'gcp_region' = 'us-central1')";
    sqlEnv.executeDdl(createCatalog);

    // 2) use the catalog we just created
    String setCatalog = "USE CATALOG my_catalog";
    sqlEnv.executeDdl(setCatalog);

    // 3) create Beam table
    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR \n "
            + ") \n"
            + "TYPE 'iceberg' \n"
            + "LOCATION '"
            + tableIdentifier
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    // 4) insert some data)
    String insertStatement =
        "INSERT INTO TEST VALUES "
            + "(123, 1.23, TRUE, TIMESTAMP '2025-05-22 20:17:40.123', 'a'), "
            + "(456, 4.56, FALSE, TIMESTAMP '2025-05-25 20:17:40.123', 'b'), "
            + "(789, 7.89, TRUE, TIMESTAMP '2025-05-28 20:17:40.123', 'c')";
    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(insertStatement));
    writePipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    // 5) read with a filter
    String selectTableStatement =
        "SELECT c_integer, c_varchar FROM TEST where "
            + "(c_boolean=TRUE and c_varchar in ('a', 'b')) or c_float > 5";
    BeamRelNode relNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    assertThat(relNode, instanceOf(BeamPushDownIOSourceRel.class));
    // Unused fields should not be projected by an IO
    assertThat(relNode.getRowType().getFieldNames(), containsInAnyOrder("c_integer", "c_varchar"));

    assertThat(
        output.getSchema(),
        equalTo(
            Schema.builder()
                .addNullableField("c_integer", INT32)
                .addNullableField("c_varchar", STRING)
                .build()));

    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(output.getSchema()).addValues(123, "a").build(),
            Row.withSchema(output.getSchema()).addValues(789, "c").build());
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(PipelineResult.State.DONE));
  }

  private Row nestedRow(List<String> arr, Integer intVal) {
    return Row.withSchema(NESTED_SCHEMA).addValues(arr, intVal).build();
  }
}
