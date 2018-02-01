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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.Context;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/** A fake dataset service that can be serialized, for use in testReadFromTable. */
class FakeDatasetService implements DatasetService, Serializable {
  // Table information must be static, as each ParDo will get a separate instance of
  // FakeDatasetServices, and they must all modify the same storage.
  static com.google.common.collect.Table<String, String, Map<String, TableContainer>>
      tables;

  Map<String, List<String>> insertErrors = Maps.newHashMap();

  public static void setUp() {
    tables = HashBasedTable.create();
    FakeJobService.setUp();
  }

  @Override
  public Table getTable(TableReference tableRef)
      throws InterruptedException, IOException {
    synchronized (tables) {
      Map<String, TableContainer> dataset =
              tables.get(tableRef.getProjectId(), tableRef.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such dataset was set",
            tableRef.getProjectId(),
            tableRef.getDatasetId());
      }
      TableContainer tableContainer = dataset.get(tableRef.getTableId());
      return tableContainer == null ? null : tableContainer.getTable();
    }
  }

  List<TableRow> getAllRows(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (tables) {
      return getTableContainer(projectId, datasetId, tableId).getRows();
    }
  }

  private TableContainer getTableContainer(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (tables) {
      Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such dataset was set",
            projectId,
            datasetId);
      }
      TableContainer tableContainer = dataset.get(tableId);
      if (tableContainer == null) {
        throwNotFound(
            "Tried to get a table %s:%s.%s, but no such table was set",
            projectId,
            datasetId,
            tableId);
      }
      return tableContainer;
    }
  }

  @Override
  public void deleteTable(TableReference tableRef) throws IOException, InterruptedException {
    validateWholeTableReference(tableRef);
    synchronized (tables) {
      Map<String, TableContainer> dataset =
          tables.get(tableRef.getProjectId(), tableRef.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableRef.getProjectId(),
            tableRef.getDatasetId());
      }
      dataset.remove(tableRef.getTableId());
    }
  }

  /**
   * Validates a table reference for whole-table operations, such as create/delete/patch. Such
   * operations do not support partition decorators.
   */
  private static void validateWholeTableReference(TableReference tableReference)
      throws IOException {
    final Pattern tableRegexp = Pattern.compile("[-\\w]{1,1024}");
    if (!tableRegexp.matcher(tableReference.getTableId()).matches()) {
      throw new IOException(
          String.format(
              "invalid table ID %s. Table IDs must be alphanumeric "
                  + "(plus underscores) and must be at most 1024 characters long. Also, table"
                  + " decorators cannot be used.",
              tableReference.getTableId()));
    }
  }

  @Override
  public void createTable(Table table) throws IOException {
    TableReference tableReference = table.getTableReference();
    validateWholeTableReference(tableReference);
    synchronized (tables) {
      Map<String, TableContainer> dataset =
          tables.get(tableReference.getProjectId(), tableReference.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableReference.getProjectId(),
            tableReference.getDatasetId());
      }
      TableContainer tableContainer =
          dataset.computeIfAbsent(tableReference.getTableId(), k -> new TableContainer(table));
    }
  }

  @Override
  public boolean isTableEmpty(TableReference tableRef)
      throws IOException, InterruptedException {
    Long numBytes = getTable(tableRef).getNumBytes();
    return numBytes == null || numBytes == 0L;
  }

  @Override
  public Dataset getDataset(
      String projectId, String datasetId) throws IOException, InterruptedException {
    synchronized (tables) {
      Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        throwNotFound("Tried to get a dataset %s:%s, but no such table was set",
                    projectId, datasetId);
      }
      return new Dataset().setDatasetReference(new DatasetReference()
          .setDatasetId(datasetId)
          .setProjectId(projectId));
    }
  }

  @Override
  public void createDataset(
      String projectId,
      String datasetId,
      String location,
      String description,
      Long defaultTableExpirationMs /* ignored */)
      throws IOException, InterruptedException {
    synchronized (tables) {
      Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        dataset = new HashMap<>();
        tables.put(projectId, datasetId, dataset);
      }
    }
  }

  @Override
  public void deleteDataset(String projectId, String datasetId)
      throws IOException, InterruptedException {
    synchronized (tables) {
      tables.remove(projectId, datasetId);
    }
  }

  public long insertAll(TableReference ref, List<TableRow> rowList,
                        @Nullable List<String> insertIdList)
      throws IOException, InterruptedException {
    List<ValueInSingleWindow<TableRow>> windowedRows = Lists.newArrayList();
    for (TableRow row : rowList) {
      windowedRows.add(ValueInSingleWindow.of(row, GlobalWindow.TIMESTAMP_MAX_VALUE,
          GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING));
    }
    return insertAll(ref, windowedRows, insertIdList, InsertRetryPolicy.alwaysRetry(), null);
  }

  @Override
  public long insertAll(
      TableReference ref, List<ValueInSingleWindow<TableRow>> rowList,
      @Nullable List<String> insertIdList,
      InsertRetryPolicy retryPolicy, List<ValueInSingleWindow<TableRow>> failedInserts)
      throws IOException, InterruptedException {
    Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> insertErrors = getInsertErrors();
    synchronized (tables) {
      if (insertIdList != null) {
        assertEquals(rowList.size(), insertIdList.size());
      } else {
        insertIdList = Lists.newArrayListWithExpectedSize(rowList.size());
        for (int i = 0; i < rowList.size(); ++i) {
          insertIdList.add(Integer.toString(ThreadLocalRandom.current().nextInt()));
        }
      }

      long dataSize = 0;
      TableContainer tableContainer = getTableContainer(
          ref.getProjectId(),
          ref.getDatasetId(),
          BigQueryHelpers.stripPartitionDecorator(ref.getTableId()));
      for (int i = 0; i < rowList.size(); ++i) {
        TableRow row = rowList.get(i).getValue();
        List<TableDataInsertAllResponse.InsertErrors> allErrors = insertErrors.get(row);
        boolean shouldInsert = true;
        if (allErrors != null) {
          for (TableDataInsertAllResponse.InsertErrors errors : allErrors) {
            if (!retryPolicy.shouldRetry(new Context(errors))) {
              shouldInsert = false;
            }
          }
        }
        if (shouldInsert) {
          dataSize += tableContainer.addRow(row, insertIdList.get(i));
        } else {
          failedInserts.add(rowList.get(i));
        }
      }
      return dataSize;
    }
  }

  @Override
  public Table patchTableDescription(TableReference tableReference,
                                     @Nullable String tableDescription)
      throws IOException, InterruptedException {
    validateWholeTableReference(tableReference);
    synchronized (tables) {
      TableContainer tableContainer = getTableContainer(tableReference.getProjectId(),
          tableReference.getDatasetId(), tableReference.getTableId());
      tableContainer.getTable().setDescription(tableDescription);
      return tableContainer.getTable();
    }
  }

  /**
   * Cause a given {@link TableRow} object to fail when it's inserted. The errors link the list
   * will be returned on subsequent retries, and the insert will succeed when the errors run out.
   */
  public void failOnInsert(
      Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> insertErrors) {
    synchronized (tables) {
      for (Map.Entry<TableRow, List<TableDataInsertAllResponse.InsertErrors>> entry
          : insertErrors.entrySet()) {
        List<String> errorStrings = Lists.newArrayList();
        for (TableDataInsertAllResponse.InsertErrors errors : entry.getValue()) {
          errorStrings.add(BigQueryHelpers.toJsonString(errors));
        }
        this.insertErrors.put(BigQueryHelpers.toJsonString(entry.getKey()), errorStrings);
      }
    }
  }

  Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> getInsertErrors() {
    Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> parsedInsertErrors =
        Maps.newHashMap();
    synchronized (tables) {
      for (Map.Entry<String, List<String>> entry : this.insertErrors.entrySet()) {
        TableRow tableRow = BigQueryHelpers.fromJsonString(entry.getKey(), TableRow.class);
        List<TableDataInsertAllResponse.InsertErrors> allErrors = Lists.newArrayList();
        for (String errorsString : entry.getValue()) {
          allErrors.add(BigQueryHelpers.fromJsonString(
              errorsString, TableDataInsertAllResponse.InsertErrors.class));
        }
        parsedInsertErrors.put(tableRow, allErrors);
      }
    }
    return parsedInsertErrors;
  }

  void throwNotFound(String format, Object... args) throws IOException {
    throw new IOException(
        String.format(format, args),
        new GoogleJsonResponseException.Builder(404, String.format(format, args), new HttpHeaders())
            .build());
  }
}
