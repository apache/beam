package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.Dataset;
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
          checkNotNull(
              BigQueryIOTest.tables.get(tableRef.getProjectId(), tableRef.getDatasetId()),
              "Tried to get a dataset %s:%s from %s, but no such dataset was set",
              tableRef.getProjectId(),
              tableRef.getDatasetId(),
              tableRef.getTableId(),
              FakeDatasetService.class.getSimpleName());
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
       Map<String, TableContainer> dataset =
           checkNotNull(
               BigQueryIOTest.tables.get(projectId, datasetId),
               "Tried to get a dataset %s:%s from %s, but no such dataset was set",
               projectId,
               datasetId,
               FakeDatasetService.class.getSimpleName());
       return checkNotNull(dataset.get(tableId),
           "Tried to get a table %s:%s.%s from %s, but no such table was set",
           projectId,
           datasetId,
           tableId,
           FakeDatasetService.class.getSimpleName());
     }
  }

  @Override
  public void deleteTable(TableReference tableRef) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Unsupported");
  }


  @Override
  public void createTable(Table table) throws IOException {
    TableReference tableReference = table.getTableReference();
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset =
          checkNotNull(
              BigQueryIOTest.tables.get(tableReference.getProjectId(),
                  tableReference.getDatasetId()),
              "Tried to get a dataset %s:%s from %s, but no such table was set",
              tableReference.getProjectId(),
              tableReference.getDatasetId(),
              FakeDatasetService.class.getSimpleName());
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
    throw new UnsupportedOperationException("Unsupported");
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
    throw new UnsupportedOperationException("Unsupported");
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
        tableContainer.addRow(rowList.get(i), insertIdList.get(i));
        dataSize += rowList.get(i).toString().length();
      }
      return dataSize;
    }
  }

  @Override
  public Table patchTableDescription(TableReference tableReference,
                                     @Nullable String tableDescription)
      throws IOException, InterruptedException {
    synchronized (BigQueryIOTest.tables) {
      Map<String, TableContainer> dataset =
          checkNotNull(
              BigQueryIOTest.tables.get(tableReference.getProjectId(),
                  tableReference.getDatasetId()),
              "Tried to get a dataset %s:%s from %s, but no such dataset was set",
              tableReference.getProjectId(),
              tableReference.getDatasetId(),
              tableReference.getTableId(),
              FakeDatasetService.class.getSimpleName());
      TableContainer tableContainer = checkNotNull(dataset.get(tableReference.getTableId()),
          "Tried to patch a table %s:%s.%s from %s, but no such table was set",
          tableReference.getProjectId(),
          tableReference.getDatasetId(),
          tableReference.getTableId(),
          FakeDatasetService.class.getSimpleName());
      tableContainer.getTable().setDescription(tableDescription);
      return tableContainer.getTable();
    }
  }
}
