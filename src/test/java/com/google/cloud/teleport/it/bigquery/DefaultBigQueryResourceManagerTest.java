/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultBigQueryResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BigQuery bigQuery;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Dataset dataset;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Table table;

  @Mock private Credentials credentials;
  @Mock private Page<Table> tables;
  @Mock private Schema schema;
  @Mock private InsertAllRequest.RowToInsert rowToInsert;
  @Mock private TableResult tableResult;
  @Mock private FieldValueList rowToRead;
  @Mock private InsertAllResponse insertResponse;
  @Mock private Map<Long, List<BigQueryError>> writeErrorsMap;

  private static final String TABLE_NAME = "table-name";
  private static final String DATASET_ID = "dataset-id";
  private static final String TEST_ID = "test-id";
  private static final String PROJECT_ID = "test-project";
  private static final String REGION = "us-central1";

  private DefaultBigQueryResourceManager testManager;

  @Before
  public void setUp() {
    testManager = new DefaultBigQueryResourceManager(TEST_ID, PROJECT_ID, bigQuery);
  }

  @Test
  public void testGetProjectIdReturnsCorrectValue() {
    assertThat(testManager.getProjectId()).isEqualTo(PROJECT_ID);
  }

  @Test
  public void testGetDatasetIdReturnsCorrectValue() {
    DefaultBigQueryResourceManager.Builder tmBuilder =
        DefaultBigQueryResourceManager.builder(TEST_ID, PROJECT_ID);
    tmBuilder.setCredentials(credentials);
    DefaultBigQueryResourceManager tm = tmBuilder.build();

    assertThat(tm.getDatasetId()).matches(TEST_ID.replace('-', '_') + "_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testCreateDatasetShouldThrowErrorWhenDatasetCreateFails() {
    when(bigQuery.create(any(DatasetInfo.class))).thenThrow(RuntimeException.class);

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.createDataset(DATASET_ID));
  }

  @Test
  public void testCreateDatasetShouldNotCreateDatasetWhenDatasetAlreadyExists() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);

    testManager.createDataset(DATASET_ID);

    assertThrows(IllegalStateException.class, () -> testManager.createDataset(DATASET_ID));
  }

  @Test
  public void testCreateDatasetShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);

    testManager.createDataset(DATASET_ID);
    verify(bigQuery).create(any(DatasetInfo.class));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableNameIsNotValid() {
    assertThrows(IllegalArgumentException.class, () -> testManager.createTable("", schema));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenSchemaIsNull() {
    assertThrows(IllegalArgumentException.class, () -> testManager.createTable(TABLE_NAME, null));
  }

  @Test
  public void testCreateTableShouldCreateDatasetWhenDatasetDoesNotExist() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    verify(bigQuery).create(any(DatasetInfo.class));
    verify(bigQuery).create(any(TableInfo.class));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenCreateFails() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    testManager.createDataset(DATASET_ID);

    when(bigQuery.create(any(TableInfo.class))).thenThrow(BigQueryException.class);

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.createTable(TABLE_NAME, schema));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableExists() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);

    testManager.createDataset(DATASET_ID);

    when(bigQuery.getTable(any())).thenReturn(any());

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.createTable(TABLE_NAME, schema));
  }

  @Test
  public void testCreateTableShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    verify(bigQuery).create(any(DatasetInfo.class));
  }

  @Test
  public void testWriteShouldThrowErrorWhenDatasetDoesNotExist() {
    assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, rowToInsert));
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert)));
  }

  @Test
  public void testWriteShouldThrowErrorWhenTableDoesNotExist() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);

    testManager.createDataset(DATASET_ID);

    when(dataset.get(anyString())).thenReturn(null);

    assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, rowToInsert));
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert)));
  }

  @Test
  public void testWriteShouldThrowErrorWhenInsertFails() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    testManager.createDataset(DATASET_ID);

    when(dataset.get(anyString())).thenReturn(table);
    when(table.insert(any())).thenThrow(BigQueryException.class);

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.write(TABLE_NAME, rowToInsert));
    assertThrows(
        BigQueryResourceManagerException.class,
        () -> testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert)));
  }

  @Test
  public void testWriteErrorsShouldBeLogged() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    when(table.insert(any())).thenReturn(insertResponse);
    when(insertResponse.hasErrors()).thenReturn(true);
    when(insertResponse.getInsertErrors()).thenReturn(writeErrorsMap);

    HashMap.SimpleEntry<Long, List<BigQueryError>> errorEntry =
        new HashMap.SimpleEntry<>(1L, ImmutableList.of(new BigQueryError("", "", "")));
    HashSet<HashMap.Entry<Long, List<BigQueryError>>> errorSet = new HashSet<>();
    errorSet.add(errorEntry);
    when(writeErrorsMap.entrySet()).thenReturn(errorSet);

    testManager.write(TABLE_NAME, rowToInsert);
    testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert));
    verify(writeErrorsMap, new Times(2)).entrySet();
  }

  @Test
  public void testWriteShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    testManager.write(TABLE_NAME, rowToInsert);
    testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert));
    verify(table, new Times(2)).insert(any());
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDatasetDoesNotExist() {
    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenTableDoesNotExist() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);

    testManager.createDataset(REGION);
    when(dataset.get(anyString())).thenReturn(null);
    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenBigQueryFailsToReadRows() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    when(bigQuery.listTableData(any())).thenThrow(BigQueryException.class);

    assertThrows(BigQueryResourceManagerException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    when(bigQuery.listTableData(table.getTableId())).thenReturn(tableResult);
    when(tableResult.getValues()).thenReturn(ImmutableList.of(rowToRead));

    testManager.readTable(TABLE_NAME);

    verify(bigQuery).listTableData(any());
  }

  @Test
  public void testCleanupShouldThrowErrorWhenTableDeleteFails() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);
    when(table.getTableId().getTable()).thenReturn(TABLE_NAME);

    testManager.createDataset(DATASET_ID);

    when(tables.iterateAll()).thenReturn(ImmutableList.of(table));
    when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tables);
    when(bigQuery.delete(any(TableId.class))).thenThrow(BigQueryException.class);

    assertThrows(BigQueryResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupShouldThrowErrorWhenDatasetDeleteFails() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);
    when(table.getTableId().getTable()).thenReturn(TABLE_NAME);

    testManager.createDataset(DATASET_ID);

    when(tables.iterateAll()).thenReturn(ImmutableList.of(table));
    when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tables);
    when(bigQuery.delete(any(DatasetId.class))).thenThrow(BigQueryException.class);

    assertThrows(BigQueryResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    // Use mocked dataset object
    when(bigQuery.create(any(DatasetInfo.class))).thenReturn(dataset);
    when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    // Use mocked table object
    when(dataset.get(any())).thenReturn(table);
    when(bigQuery.getTable(any())).thenReturn(null);
    when(table.getTableId().getTable()).thenReturn(TABLE_NAME);

    testManager.createDataset(DATASET_ID);

    when(tables.iterateAll()).thenReturn(ImmutableList.of(table));
    when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tables);

    testManager.cleanupAll();

    verify(bigQuery).delete(any(TableId.class));
    verify(bigQuery).delete(any(DatasetId.class));
  }
}
