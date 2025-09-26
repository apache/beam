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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** UnitTest for {@link IcebergMetastore}. */
public class IcebergMetastoreTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private IcebergCatalog catalog;

  @Before
  public void setup() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    assertTrue(warehouseFile.delete());
    String warehouse = "file:" + warehouseFile + "/" + UUID.randomUUID();
    catalog =
        new IcebergCatalog(
            "test_catalog", ImmutableMap.of("type", "hadoop", "warehouse", warehouse));
  }

  private IcebergMetastore metastore() {
    return catalog.metaStore(catalog.currentDatabase());
  }

  @Test
  public void testGetTableType() {
    assertEquals("iceberg", metastore().getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() {
    Table table = Table.builder().name("my_table").schema(Schema.of()).type("iceberg").build();
    BeamSqlTable sqlTable = metastore().buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof IcebergTable);

    IcebergTable icebergTable = (IcebergTable) sqlTable;
    assertEquals(catalog.currentDatabase() + ".my_table", icebergTable.tableIdentifier);
    assertEquals(catalog.catalogConfig, icebergTable.catalogConfig);
  }

  @Test
  public void testCreateTable() {
    Table table = Table.builder().name("my_table").schema(Schema.of()).type("iceberg").build();
    metastore().createTable(table);

    assertNotNull(catalog.catalogConfig.loadTable(catalog.currentDatabase() + ".my_table"));
  }

  @Test
  public void testGetTables() {
    Table table1 = Table.builder().name("my_table_1").schema(Schema.of()).type("iceberg").build();
    Table table2 = Table.builder().name("my_table_2").schema(Schema.of()).type("iceberg").build();
    metastore().createTable(table1);
    metastore().createTable(table2);

    assertEquals(ImmutableSet.of("my_table_1", "my_table_2"), metastore().getTables().keySet());
  }

  @Test
  public void testSupportsPartitioning() {
    Table table = Table.builder().name("my_table_1").schema(Schema.of()).type("iceberg").build();
    assertTrue(metastore().supportsPartitioning(table));
  }
}
