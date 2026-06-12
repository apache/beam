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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link TableCache}. */
public class TableCacheTest {
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "table");

  @Before
  public void setUp() {
    TableCache.invalidateAll();
  }

  @After
  public void tearDown() {
    TableCache.invalidateAll();
  }

  @Test
  public void getLoadsTableOnceForSameCatalogAndIdentifier() {
    Catalog catalog = mock(Catalog.class);
    IcebergCatalogConfig catalogConfig = mock(IcebergCatalogConfig.class);
    Table table = mock(Table.class);
    when(catalogConfig.catalog()).thenReturn(catalog);
    when(catalog.loadTable(IDENTIFIER)).thenReturn(table);

    assertSame(table, TableCache.get(catalogConfig, IDENTIFIER));
    assertSame(table, TableCache.get(catalogConfig, IDENTIFIER));
    assertSame(table, TableCache.get(catalogConfig, IDENTIFIER));

    verify(catalog, times(1)).loadTable(IDENTIFIER);
  }

  @Test
  public void getKeysByCatalogConfigAndIdentifier() throws Exception {
    IcebergCatalogConfig catalogConfig1 =
        IcebergCatalogConfig.builder().setCatalogName("catalog").build();
    IcebergCatalogConfig catalogConfig2 =
        IcebergCatalogConfig.builder().setCatalogName("catalog").build();
    Table table = mock(Table.class);
    AtomicInteger loadCount = new AtomicInteger();

    assertSame(
        table,
        TableCache.get(
            catalogConfig1,
            IDENTIFIER,
            () -> {
              loadCount.incrementAndGet();
              return table;
            }));
    assertSame(
        table,
        TableCache.get(
            catalogConfig2,
            IDENTIFIER,
            () -> {
              loadCount.incrementAndGet();
              return null;
            }));

    org.junit.Assert.assertEquals(1, loadCount.get());
  }

  @Test
  public void getRefreshedDoesNotRefreshNewlyLoadedTable() {
    Catalog catalog = mock(Catalog.class);
    IcebergCatalogConfig catalogConfig = mock(IcebergCatalogConfig.class);
    Table table = mock(Table.class);
    when(catalogConfig.catalog()).thenReturn(catalog);
    when(catalog.loadTable(IDENTIFIER)).thenReturn(table);

    assertSame(table, TableCache.getRefreshed(catalogConfig, IDENTIFIER));

    verify(catalog, times(1)).loadTable(IDENTIFIER);
    verify(table, never()).refresh();
  }

  @Test
  public void getRefreshedPropagatesRefreshFailure() {
    IcebergCatalogConfig catalogConfig = mock(IcebergCatalogConfig.class);
    Table table = mock(Table.class);
    RuntimeException refreshFailure = new RuntimeException("refresh failed");
    doThrow(refreshFailure).when(table).refresh();
    TableCache.put(catalogConfig, IDENTIFIER, table, Instant.EPOCH);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> TableCache.getRefreshed(catalogConfig, IDENTIFIER));

    assertSame(refreshFailure, thrown);
  }
}
