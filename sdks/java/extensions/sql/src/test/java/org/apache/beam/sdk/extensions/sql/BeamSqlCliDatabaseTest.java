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

import static org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog.DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.runtime.CalciteContextException;
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
        ImmutableSet.of(DEFAULT, "my_database"), catalogManager.currentCatalog().listDatabases());
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
        ImmutableSet.of(DEFAULT, "my_database"), catalogManager.currentCatalog().listDatabases());
  }

  @Test
  public void testUseDatabase() {
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());
    cli.execute("CREATE DATABASE my_database");
    cli.execute("CREATE DATABASE my_database2");
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());
    cli.execute("USE DATABASE my_database");
    assertEquals("my_database", catalogManager.currentCatalog().currentDatabase());
    cli.execute("USE DATABASE my_database2");
    assertEquals("my_database2", catalogManager.currentCatalog().currentDatabase());
  }

  @Test
  public void testUseDatabase_doesNotExist() {
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot use database: 'non_existent' not found.");
    cli.execute("USE DATABASE non_existent");
  }

  @Test
  public void testDropDatabase() {
    cli.execute("CREATE DATABASE my_database");
    assertEquals(
        ImmutableSet.of(DEFAULT, "my_database"), catalogManager.currentCatalog().listDatabases());
    cli.execute("DROP DATABASE my_database");
    assertEquals(ImmutableSet.of(DEFAULT), catalogManager.currentCatalog().listDatabases());
  }

  @Test
  public void testDropDatabase_nonexistent() {
    assertEquals(ImmutableSet.of(DEFAULT), catalogManager.currentCatalog().listDatabases());
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Database 'my_database' does not exist.");
    cli.execute("DROP DATABASE my_database");
  }

  @Test
  public void testCreateInsertDropTableUsingDefaultDatabase() {
    Catalog catalog = catalogManager.currentCatalog();
    // create new database db_1
    cli.execute("CREATE DATABASE db_1");
    cli.execute("USE DATABASE db_1");
    assertEquals("db_1", catalog.currentDatabase());
    assertEquals(ImmutableSet.of(DEFAULT, "db_1"), catalog.listDatabases());

    // create new table
    TestTableProvider testTableProvider = new TestTableProvider();
    catalogManager.registerTableProvider(testTableProvider);
    cli.execute("CREATE EXTERNAL TABLE person(id int, name varchar, age int) TYPE 'test'");
    // table should be inside the currently used database
    Table table = catalog.metaStore("db_1").getTable("person");
    assertNotNull(table);

    // write to the table
    cli.execute("INSERT INTO person VALUES(123, 'John', 34)");
    TestTableProvider.TableWithRows tableWithRows = testTableProvider.tables().get(table.getName());
    assertEquals(1, tableWithRows.getRows().size());
    Row row = tableWithRows.getRows().get(0);
    Row expectedRow =
        Row.withSchema(
                Schema.builder()
                    .addNullableInt32Field("id")
                    .addNullableStringField("name")
                    .addNullableInt32Field("age")
                    .build())
            .addValues(123, "John", 34)
            .build();
    assertEquals(expectedRow, row);

    // drop table, using the current database
    cli.execute("DROP TABLE person");
    assertNull(catalogManager.currentCatalog().metaStore("db_1").getTable("person"));
  }

  @Test
  public void testCreateInsertDropTableUsingOtherDatabase() {
    Catalog catalog = catalogManager.currentCatalog();
    // create database db_1
    cli.execute("CREATE DATABASE db_1");
    cli.execute("USE DATABASE db_1");
    assertEquals("db_1", catalog.currentDatabase());
    assertEquals(ImmutableSet.of(DEFAULT, "db_1"), catalog.listDatabases());

    // switch to other database db_2
    cli.execute("CREATE DATABASE db_2");
    cli.execute("USE DATABASE db_2");
    assertEquals("db_2", catalog.currentDatabase());

    // create table from another database
    TestTableProvider testTableProvider = new TestTableProvider();
    catalogManager.registerTableProvider(testTableProvider);
    cli.execute("CREATE EXTERNAL TABLE db_1.person(id int, name varchar, age int) TYPE 'test'");
    // current database should not have the table
    assertNull(catalog.metaStore("db_2").getTable("person"));

    // other database should have the table
    Table table = catalog.metaStore("db_1").getTable("person");
    assertNotNull(table);

    // write to table from another database
    cli.execute("INSERT INTO db_1.person VALUES(123, 'John', 34)");
    TestTableProvider.TableWithRows tableWithRows = testTableProvider.tables().get(table.getName());
    assertEquals(1, tableWithRows.getRows().size());
    Row row = tableWithRows.getRows().get(0);
    Row expectedRow =
        Row.withSchema(
                Schema.builder()
                    .addNullableInt32Field("id")
                    .addNullableStringField("name")
                    .addNullableInt32Field("age")
                    .build())
            .addValues(123, "John", 34)
            .build();
    assertEquals(expectedRow, row);

    // drop table, overriding the current database
    cli.execute("DROP TABLE db_1.person");
    assertNull(catalogManager.currentCatalog().metaStore("db_1").getTable("person"));
  }
}
