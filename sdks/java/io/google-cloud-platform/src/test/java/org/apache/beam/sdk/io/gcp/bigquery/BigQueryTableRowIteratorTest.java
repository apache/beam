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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link BigQueryTableRowIterator}.
 */
@RunWith(JUnit4.class)
public class BigQueryTableRowIteratorTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private Bigquery mockClient;
  @Mock private Bigquery.Datasets mockDatasets;
  @Mock private Bigquery.Datasets.Delete mockDatasetsDelete;
  @Mock private Bigquery.Datasets.Insert mockDatasetsInsert;
  @Mock private Bigquery.Jobs mockJobs;
  @Mock private Bigquery.Jobs.Get mockJobsGet;
  @Mock private Bigquery.Jobs.Insert mockJobsInsert;
  @Mock private Bigquery.Tables mockTables;
  @Mock private Bigquery.Tables.Get mockTablesGet;
  @Mock private Bigquery.Tables.Delete mockTablesDelete;
  @Mock private Bigquery.Tabledata mockTabledata;
  @Mock private Bigquery.Tabledata.List mockTabledataList;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(mockClient.tabledata()).thenReturn(mockTabledata);
    when(mockTabledata.list(anyString(), anyString(), anyString())).thenReturn(mockTabledataList);

    when(mockClient.tables()).thenReturn(mockTables);
    when(mockTables.delete(anyString(), anyString(), anyString())).thenReturn(mockTablesDelete);
    when(mockTables.get(anyString(), anyString(), anyString())).thenReturn(mockTablesGet);

    when(mockClient.datasets()).thenReturn(mockDatasets);
    when(mockDatasets.delete(anyString(), anyString())).thenReturn(mockDatasetsDelete);
    when(mockDatasets.insert(anyString(), any(Dataset.class))).thenReturn(mockDatasetsInsert);

    when(mockClient.jobs()).thenReturn(mockJobs);
    when(mockJobs.insert(anyString(), any(Job.class))).thenReturn(mockJobsInsert);
    when(mockJobs.get(anyString(), anyString())).thenReturn(mockJobsGet);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockClient);
    verifyNoMoreInteractions(mockDatasets);
    verifyNoMoreInteractions(mockDatasetsDelete);
    verifyNoMoreInteractions(mockDatasetsInsert);
    verifyNoMoreInteractions(mockJobs);
    verifyNoMoreInteractions(mockJobsGet);
    verifyNoMoreInteractions(mockJobsInsert);
    verifyNoMoreInteractions(mockTables);
    verifyNoMoreInteractions(mockTablesDelete);
    verifyNoMoreInteractions(mockTablesGet);
    verifyNoMoreInteractions(mockTabledata);
    verifyNoMoreInteractions(mockTabledataList);
  }

  private static Table tableWithBasicSchema() {
    return new Table()
        .setSchema(
            new TableSchema()
                .setFields(
                    Arrays.asList(
                        new TableFieldSchema().setName("name").setType("STRING"),
                        new TableFieldSchema().setName("answer").setType("INTEGER"),
                        new TableFieldSchema().setName("photo").setType("BYTES"),
                        new TableFieldSchema().setName("anniversary_date").setType("DATE"),
                        new TableFieldSchema().setName("anniversary_datetime").setType("DATETIME"),
                        new TableFieldSchema().setName("anniversary_time").setType("TIME"))));
  }

  private static Table noTableQuerySchema() {
    return new Table()
        .setSchema(
            new TableSchema()
                .setFields(
                    Arrays.asList(
                        new TableFieldSchema().setName("name").setType("STRING"),
                        new TableFieldSchema().setName("count").setType("INTEGER"),
                        new TableFieldSchema().setName("photo").setType("BYTES"))));
  }

  private static Table tableWithLocation() {
    return new Table()
        .setLocation("EU");
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

  /**
   * Verifies that when the query runs, the correct data is returned and the temporary dataset and
   * table are both cleaned up.
   */
  @Test
  public void testReadFromQuery() throws IOException, InterruptedException {
    // Mock job inserting.
    Job dryRunJob = new Job().setStatistics(
        new JobStatistics().setQuery(new JobStatistics2().setReferencedTables(
            ImmutableList.of(new TableReference()))));
    Job insertedJob = new Job().setJobReference(new JobReference());
    when(mockJobsInsert.execute()).thenReturn(dryRunJob, insertedJob);

    // Mock job polling.
    JobStatus status = new JobStatus().setState("DONE");
    JobConfigurationQuery resultQueryConfig = new JobConfigurationQuery()
        .setDestinationTable(new TableReference()
            .setProjectId("project")
            .setDatasetId("tempdataset")
            .setTableId("temptable"));
    Job getJob =
        new Job()
            .setJobReference(new JobReference())
            .setStatus(status)
            .setConfiguration(new JobConfiguration().setQuery(resultQueryConfig));
    when(mockJobsGet.execute()).thenReturn(getJob);

    // Mock table schema fetch.
    when(mockTablesGet.execute()).thenReturn(tableWithLocation(), tableWithBasicSchema());

    byte[] photoBytes = "photograph".getBytes();
    String photoBytesEncoded = BaseEncoding.base64().encode(photoBytes);
    // Mock table data fetch.
    when(mockTabledataList.execute()).thenReturn(
        rawDataList(rawRow("Arthur", 42, photoBytesEncoded,
            "2000-01-01", "2000-01-01 00:00:00.000005", "00:00:00.000005")));

    // Run query and verify
    String query = "SELECT name, count, photo, anniversary_date, "
        + "anniversary_datetime, anniversary_time from table";
    JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery(query);
    try (BigQueryTableRowIterator iterator =
            BigQueryTableRowIterator.fromQuery(queryConfig, "project", mockClient)) {
      iterator.open();
      assertTrue(iterator.advance());
      TableRow row = iterator.getCurrent();

      assertTrue(row.containsKey("name"));
      assertTrue(row.containsKey("answer"));
      assertTrue(row.containsKey("photo"));
      assertTrue(row.containsKey("anniversary_date"));
      assertTrue(row.containsKey("anniversary_datetime"));
      assertTrue(row.containsKey("anniversary_time"));
      assertEquals("Arthur", row.get("name"));
      assertEquals(42, row.get("answer"));
      assertEquals(photoBytesEncoded, row.get("photo"));
      assertEquals("2000-01-01", row.get("anniversary_date"));
      assertEquals("2000-01-01 00:00:00.000005", row.get("anniversary_datetime"));
      assertEquals("00:00:00.000005", row.get("anniversary_time"));

      assertFalse(iterator.advance());
    }

    // Temp dataset created and later deleted.
    verify(mockClient, times(2)).datasets();
    verify(mockDatasets).insert(anyString(), any(Dataset.class));
    verify(mockDatasetsInsert).execute();
    verify(mockDatasets).delete(anyString(), anyString());
    verify(mockDatasetsDelete).execute();
    // Job inserted to run the query, polled once.
    verify(mockClient, times(3)).jobs();
    verify(mockJobs, times(2)).insert(anyString(), any(Job.class));
    verify(mockJobsInsert, times(2)).execute();
    verify(mockJobs).get(anyString(), anyString());
    verify(mockJobsGet).execute();
    // Temp table get after query finish, deleted after reading.
    verify(mockClient, times(3)).tables();
    verify(mockTables, times(2)).get(anyString(), anyString(), anyString());
    verify(mockTablesGet, times(2)).execute();
    verify(mockTables).delete(anyString(), anyString(), anyString());
    verify(mockTablesDelete).execute();
    // Table data read.
    verify(mockClient).tabledata();
    verify(mockTabledata).list("project", "tempdataset", "temptable");
    verify(mockTabledataList).execute();
  }

  /**
   * Verifies that queries that reference no data can be read.
   */
  @Test
  public void testReadFromQueryNoTables() throws IOException, InterruptedException {
    // Mock job inserting.
    Job dryRunJob = new Job().setStatistics(
        new JobStatistics().setQuery(new JobStatistics2()));
    Job insertedJob = new Job().setJobReference(new JobReference());
    when(mockJobsInsert.execute()).thenReturn(dryRunJob, insertedJob);

    // Mock job polling.
    JobStatus status = new JobStatus().setState("DONE");
    JobConfigurationQuery resultQueryConfig = new JobConfigurationQuery()
        .setDestinationTable(new TableReference()
            .setProjectId("project")
            .setDatasetId("tempdataset")
            .setTableId("temptable"));
    Job getJob =
        new Job()
            .setJobReference(new JobReference())
            .setStatus(status)
            .setConfiguration(new JobConfiguration().setQuery(resultQueryConfig));
    when(mockJobsGet.execute()).thenReturn(getJob);

    // Mock table schema fetch.
    when(mockTablesGet.execute()).thenReturn(noTableQuerySchema());

    byte[] photoBytes = "photograph".getBytes();
    String photoBytesEncoded = BaseEncoding.base64().encode(photoBytes);
    // Mock table data fetch.
    when(mockTabledataList.execute()).thenReturn(
        rawDataList(rawRow("Arthur", 42, photoBytesEncoded)));

    // Run query and verify
    String query = String.format(
        "SELECT \"Arthur\" as name, 42 as count, \"%s\" as photo",
        photoBytesEncoded);
    JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery(query);
    try (BigQueryTableRowIterator iterator =
        BigQueryTableRowIterator.fromQuery(queryConfig, "project", mockClient)) {
      iterator.open();
      assertTrue(iterator.advance());
      TableRow row = iterator.getCurrent();

      assertTrue(row.containsKey("name"));
      assertTrue(row.containsKey("count"));
      assertTrue(row.containsKey("photo"));
      assertEquals("Arthur", row.get("name"));
      assertEquals(42, row.get("count"));
      assertEquals(photoBytesEncoded, row.get("photo"));

      assertFalse(iterator.advance());
    }

    // Temp dataset created and later deleted.
    verify(mockClient, times(2)).datasets();
    verify(mockDatasets).insert(anyString(), any(Dataset.class));
    verify(mockDatasetsInsert).execute();
    verify(mockDatasets).delete(anyString(), anyString());
    verify(mockDatasetsDelete).execute();
    // Job inserted to run the query, polled once.
    verify(mockClient, times(3)).jobs();
    verify(mockJobs, times(2)).insert(anyString(), any(Job.class));
    verify(mockJobsInsert, times(2)).execute();
    verify(mockJobs).get(anyString(), anyString());
    verify(mockJobsGet).execute();
    // Temp table get after query finish, deleted after reading.
    verify(mockClient, times(2)).tables();
    verify(mockTables, times(1)).get(anyString(), anyString(), anyString());
    verify(mockTablesGet, times(1)).execute();
    verify(mockTables).delete(anyString(), anyString(), anyString());
    verify(mockTablesDelete).execute();
    // Table data read.
    verify(mockClient).tabledata();
    verify(mockTabledata).list("project", "tempdataset", "temptable");
    verify(mockTabledataList).execute();
  }

  /**
   * Verifies that when the query fails, the user gets a useful exception and the temporary dataset
   * is cleaned up. Also verifies that the temporary table (which is never created) is not
   * erroneously attempted to be deleted.
   */
  @Test
  public void testQueryFailed() throws IOException {
    // Job state polled with an error.
    String errorReason = "bad query";
    Exception exception = new IOException(errorReason);
    when(mockJobsInsert.execute()).thenThrow(exception, exception, exception, exception);

    JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery("NOT A QUERY");
    try (BigQueryTableRowIterator iterator =
        BigQueryTableRowIterator.fromQuery(queryConfig, "project", mockClient)) {
      iterator.open();
      fail();
    } catch (Exception expected) {
      // Verify message explains cause and reports the query.
      assertThat(expected.getMessage(), containsString("Error"));
      assertThat(expected.getMessage(), containsString("NOT A QUERY"));
      assertThat(expected.getCause().getMessage(), containsString(errorReason));
    }

    // Job inserted to run the query, then polled once.
    verify(mockClient, times(1)).jobs();
    verify(mockJobs).insert(anyString(), any(Job.class));
    verify(mockJobsInsert, times(4)).execute();
  }
}
