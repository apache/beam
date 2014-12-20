/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests for BigQueryReader.
 *
 * <p>The tests just make sure a basic scenario of reading works because the class itself is a
 * thin wrapper over {@code BigQueryTableRowIterator}. The tests for the wrapped class have
 * comprehensive coverage.
 */
@RunWith(JUnit4.class)
public class BigQueryReaderTest {
  @Mock
  private Bigquery mockClient;
  @Mock
  private Bigquery.Tables mockTables;
  @Mock
  private Bigquery.Tables.Get mockTablesGet;
  @Mock
  private Bigquery.Tabledata mockTabledata;
  @Mock
  private Bigquery.Tabledata.List mockTabledataList;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockClient);
    verifyNoMoreInteractions(mockTables);
    verifyNoMoreInteractions(mockTablesGet);
    verifyNoMoreInteractions(mockTabledata);
    verifyNoMoreInteractions(mockTabledataList);
  }

  private void onTableGet(Table table) throws IOException {
    when(mockClient.tables()).thenReturn(mockTables);
    when(mockTables.get(anyString(), anyString(), anyString())).thenReturn(mockTablesGet);
    when(mockTablesGet.execute()).thenReturn(table);
  }

  private void verifyTableGet() throws IOException {
    verify(mockClient).tables();
    verify(mockTables).get("project", "dataset", "table");
    verify(mockTablesGet).execute();
  }

  private void onTableList(TableDataList result) throws IOException {
    when(mockClient.tabledata()).thenReturn(mockTabledata);
    when(mockTabledata.list(anyString(), anyString(), anyString())).thenReturn(mockTabledataList);
    when(mockTabledataList.execute()).thenReturn(result);
  }

  private void verifyTabledataList() throws IOException {
    verify(mockClient, atLeastOnce()).tabledata();
    verify(mockTabledata, atLeastOnce()).list("project", "dataset", "table");
    verify(mockTabledataList, atLeastOnce()).execute();
    // Max results may be set when testing for an empty table.
    verify(mockTabledataList, atLeast(0)).setMaxResults(anyLong());
  }

  private Table basicTableSchema() {
    return new Table().setSchema(new TableSchema().setFields(Arrays.asList(
        new TableFieldSchema().setName("name").setType("STRING"),
        new TableFieldSchema().setName("integer").setType("INTEGER"),
        new TableFieldSchema().setName("float").setType("FLOAT"),
        new TableFieldSchema().setName("bool").setType("BOOLEAN"))));
  }

  private TableRow rawRow(Object... args) {
    List<TableCell> cells = new LinkedList<>();
    for (Object a : args) {
      cells.add(new TableCell().setV(a));
    }
    return new TableRow().setF(cells);
  }

  private TableDataList rawDataList(TableRow... rows) {
    return new TableDataList().setRows(Arrays.asList(rows));
  }

  @Test
  public void testRead() throws IOException {
    onTableGet(basicTableSchema());

    // BQ API data is always encoded as a string
    TableDataList dataList = rawDataList(
        rawRow("Arthur", "42", "3.14159", "false"), rawRow("Allison", "79", "2.71828", "true"));
    onTableList(dataList);

    BigQueryReader reader = new BigQueryReader(
        mockClient,
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table"));

    Reader.ReaderIterator<TableRow> iterator = reader.iterator();
    Assert.assertTrue(iterator.hasNext());
    TableRow row = iterator.next();

    Assert.assertEquals("Arthur", row.get("name"));
    Assert.assertEquals("42", row.get("integer"));
    Assert.assertEquals(3.14159, row.get("float"));
    Assert.assertEquals(false, row.get("bool"));

    row = iterator.next();

    Assert.assertEquals("Allison", row.get("name"));
    Assert.assertEquals("79", row.get("integer"));
    Assert.assertEquals(2.71828, row.get("float"));
    Assert.assertEquals(true, row.get("bool"));

    Assert.assertFalse(iterator.hasNext());

    verifyTableGet();
    verifyTabledataList();
  }
}
