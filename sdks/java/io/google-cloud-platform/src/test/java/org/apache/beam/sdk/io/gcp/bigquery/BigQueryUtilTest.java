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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.DatasetServiceImpl;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for util classes related to BigQuery. */
@RunWith(JUnit4.class)
public class BigQueryUtilTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private Bigquery mockClient;
  @Mock private Bigquery.Tables mockTables;
  @Mock private Bigquery.Tables.Get mockTablesGet;
  @Mock private Bigquery.Tabledata mockTabledata;
  @Mock private Bigquery.Tabledata.List mockTabledataList;
  private PipelineOptions options;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.options = PipelineOptionsFactory.create();
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockClient);
    verifyNoMoreInteractions(mockTables);
    verifyNoMoreInteractions(mockTablesGet);
    verifyNoMoreInteractions(mockTabledata);
    verifyNoMoreInteractions(mockTabledataList);
  }

  private void onInsertAll(List<List<Long>> errorIndicesSequence) throws Exception {
    when(mockClient.tabledata()).thenReturn(mockTabledata);

    final List<TableDataInsertAllResponse> responses = new ArrayList<>();
    for (List<Long> errorIndices : errorIndicesSequence) {
      List<TableDataInsertAllResponse.InsertErrors> errors = new ArrayList<>();
      for (long i : errorIndices) {
        TableDataInsertAllResponse.InsertErrors error =
            new TableDataInsertAllResponse.InsertErrors();
        error.setIndex(i);
      }
      TableDataInsertAllResponse response = new TableDataInsertAllResponse();
      response.setInsertErrors(errors);
      responses.add(response);
    }

    Bigquery.Tabledata.InsertAll mockInsertAll = mock(Bigquery.Tabledata.InsertAll.class);
    when(mockTabledata.insertAll(
            anyString(), anyString(), anyString(), any(TableDataInsertAllRequest.class)))
        .thenReturn(mockInsertAll);

    doAnswer(
            invocation -> {
              when(mockInsertAll.execute())
                  .thenReturn(
                      responses.get(0),
                      responses
                          .subList(1, responses.size())
                          .toArray(new TableDataInsertAllResponse[responses.size() - 1]));
              return mockInsertAll;
            })
        .when(mockInsertAll)
        .setPrettyPrint(false);
  }

  private void verifyInsertAll(int expectedRetries) throws IOException {
    verify(mockClient, times(expectedRetries)).tabledata();
    verify(mockTabledata, times(expectedRetries))
        .insertAll(anyString(), anyString(), anyString(), any(TableDataInsertAllRequest.class));
  }

  private void onTableGet(Table table) throws IOException {
    when(mockClient.tables()).thenReturn(mockTables);
    when(mockTables.get(anyString(), anyString(), anyString())).thenReturn(mockTablesGet);
    when(mockTablesGet.setPrettyPrint(false)).thenReturn(mockTablesGet);
    when(mockTablesGet.execute()).thenReturn(table);
  }

  private void verifyTableGet() throws IOException {
    verify(mockClient).tables();
    verify(mockTables).get("project", "dataset", "table");
    verify(mockTablesGet, atLeastOnce()).setPrettyPrint(false);
    verify(mockTablesGet, atLeastOnce()).execute();
  }

  private void onTableList(TableDataList result) throws IOException {
    when(mockClient.tabledata()).thenReturn(mockTabledata);
    when(mockTabledata.list(anyString(), anyString(), anyString())).thenReturn(mockTabledataList);
    when(mockTabledataList.setPrettyPrint(false)).thenReturn(mockTabledataList);
    when(mockTabledataList.execute()).thenReturn(result);
  }

  private Table basicTableSchema() {
    return new Table()
        .setSchema(
            new TableSchema()
                .setFields(
                    Arrays.asList(
                        new TableFieldSchema().setName("name").setType("STRING"),
                        new TableFieldSchema().setName("answer").setType("INTEGER"))));
  }

  private TableRow rawRow(Object... args) {
    List<TableCell> cells = new ArrayList<>();
    for (Object a : args) {
      cells.add(new TableCell().setV(a));
    }
    return new TableRow().setF(cells);
  }

  @Test
  public void testTableGet() throws InterruptedException, IOException {
    onTableGet(basicTableSchema());

    TableDataList dataList = new TableDataList().setTotalRows(0L);
    onTableList(dataList);

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(mockClient, options);

    services.getTable(
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table"));

    verifyTableGet();
  }

  @Test
  public void testInsertAll() throws Exception {
    // Build up a list of indices to fail on each invocation. This should result in
    // 5 calls to insertAll.
    List<List<Long>> errorsIndices = new ArrayList<>();
    errorsIndices.add(Arrays.asList(0L, 5L, 10L, 15L, 20L));
    errorsIndices.add(Arrays.asList(0L, 2L, 4L));
    errorsIndices.add(Arrays.asList(0L, 2L));
    errorsIndices.add(new ArrayList<>());
    onInsertAll(errorsIndices);

    TableReference ref = BigQueryHelpers.parseTableSpec("project:dataset.table");
    DatasetServiceImpl datasetService = new DatasetServiceImpl(mockClient, options, 5);

    List<ValueInSingleWindow<TableRow>> rows = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 25; ++i) {
      rows.add(
          ValueInSingleWindow.of(
              rawRow("foo", 1234),
              GlobalWindow.TIMESTAMP_MAX_VALUE,
              GlobalWindow.INSTANCE,
              PaneInfo.ON_TIME_AND_ONLY_FIRING));
      ids.add("");
    }

    long totalBytes = 0;
    try {
      totalBytes =
          datasetService.insertAll(
              ref, rows, ids, InsertRetryPolicy.alwaysRetry(), null, null, false, false, false);
    } finally {
      verifyInsertAll(5);
      // Each of the 25 rows has 1 byte for length and 30 bytes: '{"f":[{"v":"foo"},{"v":1234}]}'
      assertEquals("Incorrect byte count", 25L * 31L, totalBytes);
    }
  }
}
