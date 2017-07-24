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
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;

/** A fake dataset service that can be serialized, for use in testReadFromTable. */
class FakeDatasetService implements DatasetService, Serializable {
  @Override
  public Table getTable(TableReference tableRef)
      throws InterruptedException, IOException {
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset =
              BigQueryIOTest.tables.get(tableRef.getProjectId(), tableRef.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s from, but no such dataset was set",
            tableRef.getProjectId(),
            tableRef.getDatasetId());
      }
      TableContainer tableContainer = dataset.get(tableRef.getTableId());
      return tableContainer == null ? null : tableContainer.getTable();
    }
  }

  List<TableRow> getAllRows(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (BigQueryIOTest.tables) {
      return getTableContainer(projectId, datasetId, tableId).getRows();
    }
  }

  private TableContainer getTableContainer(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset = BigQueryIOTest.tables.get(projectId, datasetId);
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
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset =
          BigQueryIOTest.tables.get(tableRef.getProjectId(), tableRef.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableRef.getProjectId(),
            tableRef.getDatasetId());
      }
      dataset.remove(tableRef.getTableId());
    }
  }


  @Override
  public void createTable(Table table) throws IOException {
    TableReference tableReference = table.getTableReference();
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset =
          BigQueryIOTest.tables.get(tableReference.getProjectId(), tableReference.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableReference.getProjectId(),
            tableReference.getDatasetId());
      }
      TableContainer tableContainer = dataset.get(tableReference.getTableId());
      if (tableContainer == null) {
        tableContainer = new TableContainer(table);
        dataset.put(tableReference.getTableId(), tableContainer);
      }
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
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset = BigQueryIOTest.tables.get(projectId, datasetId);
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
      String projectId, String datasetId, String location, String description)
      throws IOException, InterruptedException {
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset = BigQueryIOTest.tables.get(projectId, datasetId);
      if (dataset == null) {
        dataset = new HashMap<>();
        BigQueryIOTest.tables.put(projectId, datasetId, dataset);
      }
    }
  }

  @Override
  public void deleteDataset(String projectId, String datasetId)
      throws IOException, InterruptedException {
    synchronized (BigQueryIOTest.tables) {
      BigQueryIOTest.tables.remove(projectId, datasetId);
    }
  }

  @Override
  public long insertAll(
      TableReference ref, List<TableRow> rowList, @Nullable List<String> insertIdList)
      throws IOException, InterruptedException {
    synchronized (BigQueryIOTest.tables) {
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
          ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
      for (int i = 0; i < rowList.size(); ++i) {
        dataSize += tableContainer.addRow(rowList.get(i), insertIdList.get(i));
      }
      return dataSize;
    }
  }

  @Override
  public Table patchTableDescription(TableReference tableReference,
                                     @Nullable String tableDescription)
      throws IOException, InterruptedException {
    synchronized (BigQueryIOTest.tables) {
      TableContainer tableContainer = getTableContainer(tableReference.getProjectId(),
          tableReference.getDatasetId(), tableReference.getTableId());
      tableContainer.getTable().setDescription(tableDescription);
      return tableContainer.getTable();
    }
  }

  void throwNotFound(String format, Object... args) throws IOException {
    throw new IOException(
        new GoogleJsonResponseException.Builder(404,
            String.format(format, args), new HttpHeaders()).build());
  }
}
