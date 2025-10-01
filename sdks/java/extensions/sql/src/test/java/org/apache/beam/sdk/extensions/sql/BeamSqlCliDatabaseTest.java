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
package org.apache.beam.sdk.extensions.sql;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** UnitTest for {@link BeamSqlCli} using databases. */
public class BeamSqlCliDatabaseTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private InMemoryCatalogManager catalogManager;
  private BeamSqlCli cli;

  @Before
  public void setupCli() {
    catalogManager = new InMemoryCatalogManager();
    cli = new BeamSqlCli().catalogManager(catalogManager);
  }

  @Test
  public void testCreateDatabase() {
    cli.execute("CREATE DATABASE my_database");
    assertEquals(
        ImmutableSet.of("default", "my_database"), catalogManager.currentCatalog().listDatabases());
  }

  @Test
  public void testCreateDuplicateDatabase_error() {
    cli.execute("CREATE DATABASE my_database");
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Database 'my_database' already exists.");
    cli.execute("CREATE DATABASE my_database");
  }

  @Test
  public void testCreateDuplicateDatabase_ifNotExists() {
    cli.execute("CREATE DATABASE my_database");
    cli.execute("CREATE DATABASE IF NOT EXISTS my_database");
    assertEquals(
        ImmutableSet.of("default", "my_database"), catalogManager.currentCatalog().listDatabases());
  }

  @Test
  public void testUseDatabase() {
    assertEquals("default", catalogManager.currentCatalog().currentDatabase());
    cli.execute("CREATE DATABASE my_database");
    cli.execute("CREATE DATABASE my_database2");
    assertEquals("default", catalogManager.currentCatalog().currentDatabase());
    cli.execute("USE DATABASE my_database");
    assertEquals("my_database", catalogManager.currentCatalog().currentDatabase());
    cli.execute("USE DATABASE my_database2");
    assertEquals("my_database2", catalogManager.currentCatalog().currentDatabase());
  }

  @Test
  public void testUseDatabase_doesNotExist() {
    assertEquals("default", catalogManager.currentCatalog().currentDatabase());
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot use database: 'non_existent' not found.");
    cli.execute("USE DATABASE non_existent");
  }

  @Test
  public void testDropDatabase() {
    cli.execute("CREATE DATABASE my_database");
    assertEquals(
        ImmutableSet.of("default", "my_database"), catalogManager.currentCatalog().listDatabases());
    cli.execute("DROP DATABASE my_database");
    assertEquals(ImmutableSet.of("default"), catalogManager.currentCatalog().listDatabases());
  }

  @Test
  public void testDropDatabase_nonexistent() {
    assertEquals(ImmutableSet.of("default"), catalogManager.currentCatalog().listDatabases());
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Database 'my_database' does not exist.");
    cli.execute("DROP DATABASE my_database");
  }
}
