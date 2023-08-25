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
package org.apache.beam.it.gcp.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link BigQueryResourceManager}. */
@RunWith(JUnit4.class)
public class BigQueryResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BigQuery bigQuery;

  private Schema schema;
  private RowToInsert rowToInsert;
  private static final String TABLE_NAME = "table-name";
  private static final String DATASET_ID = "dataset-id";
  private static final String TEST_ID = "test-id";
  private static final String PROJECT_ID = "test-project";
  private static final String REGION = "us-central1";

  private BigQueryResourceManager testManager;

  @Before
  public void setUp() {
    schema = Schema.of(Field.of("name", StandardSQLTypeName.STRING));
    rowToInsert = RowToInsert.of("1", ImmutableMap.of("name", "Jake"));
    testManager = new BigQueryResourceManager(TEST_ID, PROJECT_ID, bigQuery);
  }

  @Test
  public void testGetProjectIdReturnsCorrectValue() {
    assertThat(new BigQueryResourceManager(TEST_ID, PROJECT_ID, bigQuery).getProjectId())
        .isEqualTo(PROJECT_ID);
  }

  @Test
  public void testGetDatasetIdReturnsCorrectValue() {
    BigQueryResourceManager tm = BigQueryResourceManager.builder(TEST_ID, PROJECT_ID, null).build();

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
    testManager.createDataset(DATASET_ID);

    assertThrows(IllegalStateException.class, () -> testManager.createDataset(DATASET_ID));
  }

  @Test
  public void testCreateDatasetShouldWorkWhenBigQueryDoesNotThrowAnyError() {
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
    when(bigQuery.create(any(DatasetInfo.class)).getDatasetId().getDataset())
        .thenReturn(DATASET_ID);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    verify(bigQuery).create(any(DatasetInfo.class));
    verify(bigQuery).create(any(TableInfo.class));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenCreateFails() {
    testManager.createDataset(DATASET_ID);
    when(bigQuery.create(any(TableInfo.class))).thenThrow(BigQueryException.class);

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.createTable(TABLE_NAME, schema));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableExists() {
    testManager.createDataset(DATASET_ID);

    when(bigQuery.getTable(any())).thenReturn(any());

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.createTable(TABLE_NAME, schema));
  }

  @Test
  public void testCreateTableShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    when(bigQuery.create(any(DatasetInfo.class)).getDatasetId().getDataset())
        .thenReturn(DATASET_ID);
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
    testManager.createDataset(DATASET_ID);

    when(bigQuery.create(any(DatasetInfo.class)).get(anyString())).thenReturn(null);

    assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, rowToInsert));
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert)));
  }

  @Test
  public void testWriteShouldThrowErrorWhenInsertFails() {
    Dataset mockDataset = bigQuery.create(any(DatasetInfo.class));
    when(mockDataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);

    testManager.createDataset(DATASET_ID);

    when(mockDataset.get(anyString()).insert(any())).thenThrow(BigQueryException.class);

    assertThrows(
        BigQueryResourceManagerException.class, () -> testManager.write(TABLE_NAME, rowToInsert));
    assertThrows(
        BigQueryResourceManagerException.class,
        () -> testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert)));
  }

  @Test
  public void testWriteShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    when(bigQuery.create(any(DatasetInfo.class)).getDatasetId().getDataset())
        .thenReturn(DATASET_ID);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    testManager.write(TABLE_NAME, rowToInsert);
    testManager.write(TABLE_NAME, ImmutableList.of(rowToInsert));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDatasetDoesNotExist() {
    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenTableDoesNotExist() {
    testManager.createDataset(REGION);
    when(bigQuery.create(any(DatasetInfo.class)).get(anyString())).thenReturn(null);
    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenBigQueryFailsToReadRows()
      throws InterruptedException {
    when(bigQuery.create(any(DatasetInfo.class)).getDatasetId().getDataset())
        .thenReturn(DATASET_ID);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    doThrow(BigQueryException.class).when(bigQuery).query(any(QueryJobConfiguration.class));

    assertThrows(BigQueryResourceManagerException.class, () -> testManager.readTable(TABLE_NAME));
  }

  @Test
  public void testReadTableShouldWorkWhenBigQueryDoesNotThrowAnyError()
      throws InterruptedException {
    when(bigQuery.create(any(DatasetInfo.class)).getDatasetId().getDataset())
        .thenReturn(DATASET_ID);
    when(bigQuery.getTable(any())).thenReturn(null);

    testManager.createTable(TABLE_NAME, schema);

    testManager.readTable(TABLE_NAME);

    verify(bigQuery).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testCleanupShouldThrowErrorWhenTableDeleteFails() {
    testManager.createDataset(DATASET_ID);

    Table mockTable = mock(Table.class);
    when(bigQuery.listTables(any(DatasetId.class)).iterateAll())
        .thenReturn(ImmutableList.of(mockTable));
    when(bigQuery.delete(any(TableId.class))).thenThrow(BigQueryException.class);

    assertThrows(BigQueryResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupShouldThrowErrorWhenDatasetDeleteFails() {
    testManager.createDataset(DATASET_ID);
    when(bigQuery.delete(any(DatasetId.class))).thenThrow(BigQueryException.class);

    assertThrows(BigQueryResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupShouldWorkWhenBigQueryDoesNotThrowAnyError() {
    when(bigQuery.create(any(DatasetInfo.class)).getDatasetId().getDataset())
        .thenReturn(DATASET_ID);

    testManager.createDataset(DATASET_ID);
    Table mockTable = mock(Table.class, Answers.RETURNS_DEEP_STUBS);
    when(bigQuery.listTables(any(DatasetId.class)).iterateAll())
        .thenReturn(ImmutableList.of(mockTable));
    when(mockTable.getTableId().getTable()).thenReturn(TABLE_NAME);

    testManager.cleanupAll();

    verify(bigQuery).delete(any(TableId.class));
    verify(bigQuery).delete(any(DatasetId.class));
  }
}
