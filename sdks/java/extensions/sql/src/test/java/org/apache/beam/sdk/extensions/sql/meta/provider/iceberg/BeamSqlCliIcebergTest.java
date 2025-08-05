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
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;

/** UnitTest for {@link BeamSqlCli} using Iceberg catalog. */
public class BeamSqlCliIcebergTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private InMemoryCatalogManager catalogManager;
  private BeamSqlCli cli;
  private BeamSqlEnv sqlEnv;
  private String warehouse;
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    catalogManager = new InMemoryCatalogManager();
    cli = new BeamSqlCli().catalogManager(catalogManager);
    sqlEnv =
        BeamSqlEnv.builder(catalogManager)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    assertTrue(warehouseFile.delete());
    warehouse = "file:" + warehouseFile + "/" + UUID.randomUUID();
  }

  private String createCatalog(String name) {
    return createCatalog(name, null);
  }

  private String createCatalog(String name, @Nullable String warehouseOverride) {
    String ware = warehouseOverride != null ? warehouseOverride : warehouse;
    return format("CREATE CATALOG %s \n", name)
        + "TYPE iceberg \n"
        + "PROPERTIES (\n"
        + "  'type' = 'hadoop', \n"
        + format("  'warehouse' = '%s')", ware);
  }

  @Test
  public void testCreateCatalog() {
    assertEquals("default", catalogManager.currentCatalog().name());

    cli.execute(createCatalog("my_catalog"));
    assertEquals("default", catalogManager.currentCatalog().name());

    cli.execute("USE CATALOG my_catalog");
    assertEquals("my_catalog", catalogManager.currentCatalog().name());
    assertEquals("iceberg", catalogManager.currentCatalog().type());
  }

  @Test
  public void testCreateNamespace() {
    testCreateCatalog();

    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    assertEquals("default", catalog.currentDatabase());
    cli.execute("CREATE DATABASE new_namespace");
    assertEquals("new_namespace", Iterables.getOnlyElement(catalog.listDatabases()));

    // Specifies IF NOT EXISTS, so should be a no-op
    cli.execute("CREATE DATABASE IF NOT EXISTS new_namespace");
    assertEquals("new_namespace", Iterables.getOnlyElement(catalog.listDatabases()));

    // This one doesn't, so it should throw an error.
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Database 'new_namespace' already exists.");
    cli.execute("CREATE DATABASE new_namespace");

    // assert there was a database, and cleanup
    assertTrue(catalog.dropDatabase("new_namespace", true));
  }

  @Test
  public void testUseNamespace() {
    testCreateCatalog();

    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    cli.execute("CREATE DATABASE new_namespace");
    assertEquals("default", catalog.currentDatabase());
    cli.execute("USE DATABASE new_namespace");
    assertEquals("new_namespace", catalog.currentDatabase());

    // Cannot use a non-existent namespace
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot use database: 'non_existent' not found.");
    cli.execute("USE DATABASE non_existent");

    // assert there was a database, and cleanup
    assertTrue(catalog.dropDatabase("new_namespace", true));
  }

  @Test
  public void testDropNamespace() {
    testCreateCatalog();

    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    cli.execute("CREATE DATABASE new_namespace");
    cli.execute("USE DATABASE new_namespace");
    assertEquals("new_namespace", catalog.currentDatabase());
    cli.execute("DROP DATABASE new_namespace");
    assertTrue(catalog.listDatabases().isEmpty());
    assertNull(catalog.currentDatabase());

    // Drop non-existent namespace with IF EXISTS
    cli.execute("DROP DATABASE IF EXISTS new_namespace");

    // Throw an error when IF EXISTS is not specified
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Database 'new_namespace' does not exist.");
    cli.execute("DROP DATABASE new_namespace");
  }

  @Test
  public void testCrossCatalogTableWriteAndRead() throws IOException {
    // create and use catalog 1
    sqlEnv.executeDdl(createCatalog("catalog_1"));
    sqlEnv.executeDdl("USE CATALOG catalog_1");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    // create and use database inside catalog 1
    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    sqlEnv.executeDdl("CREATE DATABASE my_namespace");
    sqlEnv.executeDdl("USE DATABASE my_namespace");
    assertEquals("my_namespace", catalog.currentDatabase());
    // create and write to table inside database
    String tableIdentifier = "my_namespace.my_table";
    sqlEnv.executeDdl(
        format("CREATE EXTERNAL TABLE %s( \n", tableIdentifier)
            + "   c_integer INTEGER, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR \n "
            + ") \n"
            + "TYPE 'iceberg'\n");
    BeamRelNode insertNode =
        sqlEnv.parseQuery(
            format("INSERT INTO %s VALUES (", tableIdentifier)
                + "2147483647, "
                + "TRUE, "
                + "TIMESTAMP '2025-07-31 20:17:40.123', "
                + "'varchar' "
                + ")");
    Pipeline p1 = Pipeline.create();
    BeamSqlRelUtils.toPCollection(p1, insertNode);
    p1.run().waitUntilFinish();

    // create and use a new catalog, with a new database
    File warehouseFile2 = TEMPORARY_FOLDER.newFolder();
    assertTrue(warehouseFile2.delete());
    String warehouse2 = "file:" + warehouseFile2 + "/" + UUID.randomUUID();
    sqlEnv.executeDdl(createCatalog("catalog_2", warehouse2));
    sqlEnv.executeDdl("USE CATALOG catalog_2");
    sqlEnv.executeDdl("CREATE DATABASE other_namespace");
    sqlEnv.executeDdl("USE DATABASE other_namespace");
    assertEquals("catalog_2", catalogManager.currentCatalog().name());
    assertEquals("other_namespace", catalogManager.currentCatalog().currentDatabase());

    // insert from old catalog to new table in new catalog
    Pipeline p2 = Pipeline.create();
    p2.apply(
        SqlTransform.query("INSERT INTO other_table SELECT * FROM catalog_1.my_namespace.my_table")
            .withDdlString(
                "CREATE EXTERNAL TABLE other_table( \n"
                    + "   c_integer INTEGER, \n"
                    + "   c_boolean BOOLEAN, \n"
                    + "   c_timestamp TIMESTAMP, \n"
                    + "   c_varchar VARCHAR) \n"
                    + "TYPE 'iceberg'\n")
            .withCatalogManager(catalogManager));
    p2.run().waitUntilFinish();

    // clear PCollection from the above run
    catalogManager.clearTableProviders();

    // switch over to catalog 1 and read table inside catalog 2
    Pipeline p3 = Pipeline.create();
    PCollection<Row> output =
        p3.apply(
            SqlTransform.query("SELECT * FROM catalog_2.other_namespace.other_table")
                .withDdlString("USE DATABASE catalog_1.my_namespace")
                .withCatalogManager(catalogManager));
    p3.run().waitUntilFinish();
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    assertEquals("my_namespace", catalogManager.currentCatalog().currentDatabase());

    // validate read contents
    Schema expectedSchema =
        checkStateNotNull(catalog.catalogConfig.loadTable(tableIdentifier)).getSchema();
    assertEquals(expectedSchema, output.getSchema());
    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(expectedSchema)
                .addValues(2147483647, true, DateTime.parse("2025-07-31T20:17:40.123Z"), "varchar")
                .build());
    p3.run().waitUntilFinish();
  }
}
