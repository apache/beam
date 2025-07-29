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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** UnitTest for {@link BeamSqlCli} using Iceberg catalog. */
public class BeamSqlCliIcebergTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private InMemoryCatalogManager catalogManager;
  private BeamSqlCli cli;
  private String warehouse;
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    catalogManager = new InMemoryCatalogManager();
    cli = new BeamSqlCli().catalogManager(catalogManager);
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    warehouse = "file:" + warehouseFile + "/" + UUID.randomUUID();
  }

  private String createCatalog(String name) {
    return format("CREATE CATALOG %s \n", name)
        + "TYPE iceberg \n"
        + "PROPERTIES (\n"
        + "  'type' = 'hadoop', \n"
        + format("  'warehouse' = '%s')", warehouse);
  }

  @Test
  public void testCreateCatalog() {
    assertEquals("default", catalogManager.currentCatalog().name());

    cli.execute(createCatalog("my_catalog"));
    assertNotNull(catalogManager.getCatalog("my_catalog"));
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
}
