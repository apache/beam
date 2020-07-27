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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verifyNotNull;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonErrorContainer;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.util.MockSleeper;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.RetryBoundedBackOff;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.extensions.gcp.util.FastNanoClockAndSleeper;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.DatasetServiceImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.JobServiceImpl;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BigQueryServicesImpl}. */
@RunWith(JUnit4.class)
public class BigQueryServicesImplTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(BigQueryServicesImpl.class);
  @Mock private LowLevelHttpResponse response;
  private MockLowLevelHttpRequest request;
  private Bigquery bigquery;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Set up the MockHttpRequest for future inspection
    request =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            return response;
          }
        };
    // A mock transport that lets us mock the API responses.
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();

    // A sample BigQuery API client that uses default JsonFactory and RetryHttpInitializer.
    bigquery =
        new Bigquery.Builder(
                transport, Transport.getJsonFactory(), new RetryHttpRequestInitializer())
            .build();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds. */
  @Test
  public void testStartLoadJobSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    JobReference jobRef = new JobReference();
    jobRef.setJobId("jobId");
    jobRef.setProjectId("projectId");
    testJob.setJobReference(jobRef);

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    Sleeper sleeper = new FastNanoClockAndSleeper();
    JobServiceImpl.startJob(
        testJob,
        new ApiErrorExtractor(),
        bigquery,
        sleeper,
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff()));

    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
    expectedLogs.verifyInfo(String.format("Started BigQuery job: %s", jobRef));
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds with an already
   * exist job.
   */
  @Test
  public void testStartLoadJobSucceedsAlreadyExists() throws IOException, InterruptedException {
    Job testJob = new Job();
    JobReference jobRef = new JobReference();
    jobRef.setJobId("jobId");
    jobRef.setProjectId("projectId");
    testJob.setJobReference(jobRef);

    when(response.getStatusCode()).thenReturn(409); // 409 means already exists

    Sleeper sleeper = new FastNanoClockAndSleeper();
    JobServiceImpl.startJob(
        testJob,
        new ApiErrorExtractor(),
        bigquery,
        sleeper,
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff()));

    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
    expectedLogs.verifyNotLogged("Started BigQuery job");
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds with a retry. */
  @Test
  public void testStartLoadJobRetry() throws IOException, InterruptedException {
    Job testJob = new Job();
    JobReference jobRef = new JobReference();
    jobRef.setJobId("jobId");
    jobRef.setProjectId("projectId");
    testJob.setJobReference(jobRef);

    // First response is 403 rate limited, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(testJob));

    Sleeper sleeper = new FastNanoClockAndSleeper();
    JobServiceImpl.startJob(
        testJob,
        new ApiErrorExtractor(),
        bigquery,
        sleeper,
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff()));

    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} succeeds. */
  @Test
  public void testPollJobSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE"));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} fails. */
  @Test
  public void testPollJobFailed() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE").setErrorResult(new ErrorProto()));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} returns UNKNOWN. */
  @Test
  public void testPollJobUnknown() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus());

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.STOP_BACKOFF);

    assertEquals(null, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  @Test
  public void testGetJobSucceeds() throws Exception {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus());

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.getJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  @Test
  public void testGetJobNotFound() throws Exception {
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(404);

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.getJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(null, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  @Test
  public void testGetJobThrows() throws Exception {
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(401);

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Unable to find BigQuery job: %s", jobRef));

    jobService.getJob(jobRef, Sleeper.DEFAULT, BackOff.STOP_BACKOFF);
  }

  @Test
  public void testGetTableSucceeds() throws Exception {
    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    Table testTable = new Table();
    testTable.setTableReference(tableRef);

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(testTable));

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    Table table = datasetService.getTable(tableRef, null, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT);

    assertEquals(testTable, table);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
  }

  @Test
  public void testGetTableNotFound() throws IOException, InterruptedException {
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(404);

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");
    Table table = datasetService.getTable(tableRef, null, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT);

    assertNull(table);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  @Test
  public void testGetTableThrows() throws Exception {
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(401);

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Unable to get table: %s", tableRef.getTableId()));

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    datasetService.getTable(tableRef, null, BackOff.STOP_BACKOFF, Sleeper.DEFAULT);
  }

  @Test
  public void testIsTableEmptySucceeds() throws Exception {
    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    TableDataList testDataList = new TableDataList().setRows(ImmutableList.of(new TableRow()));

    // First response is 403 rate limited, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(testDataList));

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    assertFalse(datasetService.isTableEmpty(tableRef, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT));

    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
  }

  @Test
  public void testIsTableEmptyNoRetryForNotFound() throws IOException, InterruptedException {
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(404);

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Unable to list table data: %s", tableRef.getTableId()));

    try {
      datasetService.isTableEmpty(tableRef, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT);
    } finally {
      verify(response, times(1)).getStatusCode();
      verify(response, times(1)).getContent();
      verify(response, times(1)).getContentType();
    }
  }

  @Test
  public void testIsTableEmptyThrows() throws Exception {
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(401);

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Unable to list table data: %s", tableRef.getTableId()));

    datasetService.isTableEmpty(tableRef, BackOff.STOP_BACKOFF, Sleeper.DEFAULT);
  }

  @Test
  public void testExecuteWithRetries() throws IOException, InterruptedException {
    Table testTable = new Table();

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testTable));

    Table table =
        BigQueryServicesImpl.executeWithRetries(
            bigquery.tables().get("projectId", "datasetId", "tableId"),
            "Failed to get table.",
            Sleeper.DEFAULT,
            BackOff.STOP_BACKOFF,
            BigQueryServicesImpl.ALWAYS_RETRY);

    assertEquals(testTable, table);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  private <T> ValueInSingleWindow<T> wrapValue(T value) {
    return ValueInSingleWindow.of(
        value,
        GlobalWindow.TIMESTAMP_MAX_VALUE,
        GlobalWindow.INSTANCE,
        PaneInfo.ON_TIME_AND_ONLY_FIRING);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} retries rate limited attempts. */
  @Test
  public void testInsertRateLimitRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    // First response is 403 rate limited, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(new TableDataInsertAllResponse()));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    expectedLogs.verifyInfo("BigQuery insertAll error, retrying:");
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} retries quota exceeded attempts. */
  @Test
  public void testInsertQuotaExceededRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    // First response is 403 quota exceeded, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("quotaExceeded", 403)))
        .thenReturn(toStream(new TableDataInsertAllResponse()));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    expectedLogs.verifyInfo("BigQuery insertAll error, retrying:");
  }

  // A BackOff that makes a total of 4 attempts
  private static final FluentBackoff TEST_BACKOFF =
      FluentBackoff.DEFAULT
          .withInitialBackoff(Duration.millis(1))
          .withExponent(1)
          .withMaxRetries(3);

  /** Tests that {@link DatasetServiceImpl#insertAll} retries selected rows on failure. */
  @Test
  public void testInsertRetrySelectRows() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("row", "a")), wrapValue(new TableRow().set("row", "b")));
    List<String> insertIds = ImmutableList.of("a", "b");

    final TableDataInsertAllResponse bFailed =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors().setIndex(1L).setErrors(ImmutableList.of(new ErrorProto()))));

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(bFailed))
        .thenReturn(toStream(allRowsSucceeded));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        insertIds,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} fails gracefully when persistent issues. */
  @Test
  public void testInsertFailsGracefully() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows =
        ImmutableList.of(wrapValue(new TableRow()), wrapValue(new TableRow()));

    final TableDataInsertAllResponse row1Failed =
        new TableDataInsertAllResponse()
            .setInsertErrors(ImmutableList.of(new InsertErrors().setIndex(1L)));

    final TableDataInsertAllResponse row0Failed =
        new TableDataInsertAllResponse()
            .setInsertErrors(ImmutableList.of(new InsertErrors().setIndex(0L)));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    // Always return 200.
    when(response.getStatusCode()).thenReturn(200);
    // Return row 1 failing, then we retry row 1 as row 0, and row 0 persistently fails.
    when(response.getContent())
        .thenReturn(toStream(row1Failed))
        .thenAnswer(invocation -> toStream(row0Failed));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    // Expect it to fail.
    try {
      dataService.insertAll(
          ref,
          rows,
          null,
          BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
          new MockSleeper(),
          InsertRetryPolicy.alwaysRetry(),
          null,
          null,
          false,
          false,
          false);
      fail();
    } catch (IOException e) {
      assertThat(e, instanceOf(IOException.class));
      assertThat(e.getMessage(), containsString("Insert failed:"));
      assertThat(e.getMessage(), containsString("[{\"index\":0}]"));
    }

    // Verify the exact number of retries as well as log messages.
    verify(response, times(4)).getStatusCode();
    verify(response, times(4)).getContent();
    verify(response, times(4)).getContentType();
    expectedLogs.verifyInfo("Retrying 1 failed inserts to BigQuery");
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} will not retry other non-rate-limited,
   * non-quota-exceeded attempts.
   */
  @Test
  public void testFaielInsertOtherRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    // First response is 403 non-{rate-limited, quota-exceeded}, second response has valid payload
    // but should not
    // be invoked.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("actually forbidden", 403)))
        .thenReturn(toStream(new TableDataInsertAllResponse()));
    try {
      DatasetServiceImpl dataService =
          new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
      dataService.insertAll(
          ref,
          rows,
          null,
          BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
          new MockSleeper(),
          InsertRetryPolicy.alwaysRetry(),
          null,
          null,
          false,
          false,
          false);
      fail();
    } catch (RuntimeException e) {
      assertTrue(e instanceof RuntimeException);
    }
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} uses the supplied {@link InsertRetryPolicy},
   * and returns the list of rows not retried.
   */
  @Test
  public void testInsertRetryPolicy() throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows =
        ImmutableList.of(wrapValue(new TableRow()), wrapValue(new TableRow()));

    // First time row0 fails with a retryable error, and row1 fails with a persistent error.
    final TableDataInsertAllResponse firstFailure =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors()
                        .setIndex(0L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout"))),
                    new InsertErrors()
                        .setIndex(1L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("invalid")))));

    // Second time there is only one row, which fails with a retryable error.
    final TableDataInsertAllResponse secondFialure =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors()
                        .setIndex(0L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout")))));

    // On the final attempt, no failures are returned.
    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    // Always return 200.
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200).thenReturn(200);

    // First fail
    when(response.getContent())
        .thenReturn(toStream(firstFailure))
        .thenReturn(toStream(secondFialure))
        .thenReturn(toStream(allRowsSucceeded));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.retryTransientErrors(),
        failedInserts,
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false);
    assertEquals(1, failedInserts.size());
    expectedLogs.verifyInfo("Retrying 1 failed inserts to BigQuery");
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} respects the skipInvalidRows,
   * ignoreUnknownValues and ignoreInsertIds parameters.
   */
  @Test
  public void testSkipInvalidRowsIgnoreUnknownIgnoreInsertIdsValuesStreaming()
      throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows =
        ImmutableList.of(wrapValue(new TableRow()), wrapValue(new TableRow()));

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    // Return a 200 response each time
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(allRowsSucceeded))
        .thenReturn(toStream(allRowsSucceeded));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    // First, test with all flags disabled
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        Lists.newArrayList(),
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false);

    TableDataInsertAllRequest parsedRequest =
        fromString(request.getContentAsString(), TableDataInsertAllRequest.class);

    assertFalse(parsedRequest.getSkipInvalidRows());
    assertFalse(parsedRequest.getIgnoreUnknownValues());

    // Then with all enabled
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        Lists.newArrayList(),
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        true,
        true,
        true);

    parsedRequest = fromString(request.getContentAsString(), TableDataInsertAllRequest.class);

    assertTrue(parsedRequest.getSkipInvalidRows());
    assertTrue(parsedRequest.getIgnoreUnknownValues());
    assertNull(parsedRequest.getRows().get(0).getInsertId());
    assertNull(parsedRequest.getRows().get(1).getInsertId());
  }

  /** A helper to convert a string response back to a {@link GenericJson} subclass. */
  private static <T extends GenericJson> T fromString(String content, Class<T> clazz)
      throws IOException {
    return JacksonFactory.getDefaultInstance().fromString(content, clazz);
  }

  /** A helper to wrap a {@link GenericJson} object in a content stream. */
  private static InputStream toStream(GenericJson content) throws IOException {
    return new ByteArrayInputStream(JacksonFactory.getDefaultInstance().toByteArray(content));
  }

  /** A helper that generates the error JSON payload that Google APIs produce. */
  private static GoogleJsonErrorContainer errorWithReasonAndStatus(String reason, int status) {
    ErrorInfo info = new ErrorInfo();
    info.setReason(reason);
    info.setDomain("global");
    // GoogleJsonError contains one or more ErrorInfo objects; our utiities read the first one.
    GoogleJsonError error = new GoogleJsonError();
    error.setErrors(ImmutableList.of(info));
    error.setCode(status);
    error.setMessage(reason);
    // The actual JSON response is an error container.
    GoogleJsonErrorContainer container = new GoogleJsonErrorContainer();
    container.setError(error);
    return container;
  }

  @Test
  public void testGetErrorInfo() throws IOException {
    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    ErrorInfo info = new ErrorInfo();
    List<ErrorInfo> infoList = new ArrayList<>();
    infoList.add(info);
    info.setReason("QuotaExceeded");
    GoogleJsonError error = new GoogleJsonError();
    error.setErrors(infoList);
    HttpResponseException.Builder builder = mock(HttpResponseException.Builder.class);
    IOException validException = new GoogleJsonResponseException(builder, error);
    IOException invalidException = new IOException();
    assertEquals(info.getReason(), dataService.getErrorInfo(validException).getReason());
    assertNull(dataService.getErrorInfo(invalidException));
  }

  @Test
  public void testCreateTableSucceeds() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    Table testTable = new Table().setTableReference(ref);
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testTable));

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    Table ret =
        services.tryCreateTable(
            testTable, new RetryBoundedBackOff(0, BackOff.ZERO_BACKOFF), Sleeper.DEFAULT);
    assertEquals(testTable, ret);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /** Tests that {@link BigQueryServicesImpl} does not retry non-rate-limited attempts. */
  @Test
  public void testCreateTableDoesNotRetry() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    Table testTable = new Table().setTableReference(ref);
    // First response is 403 not-rate-limited, second response has valid payload but should not
    // be invoked.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("actually forbidden", 403)))
        .thenReturn(toStream(testTable));

    thrown.expect(GoogleJsonResponseException.class);
    thrown.expectMessage("actually forbidden");

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    try {
      services.tryCreateTable(
          testTable, new RetryBoundedBackOff(3, BackOff.ZERO_BACKOFF), Sleeper.DEFAULT);
      fail();
    } catch (IOException e) {
      verify(response, times(1)).getStatusCode();
      verify(response, times(1)).getContent();
      verify(response, times(1)).getContentType();
      throw e;
    }
  }

  /** Tests that table creation succeeds when the table already exists. */
  @Test
  public void testCreateTableSucceedsAlreadyExists() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("column1").setType("String"),
                    new TableFieldSchema().setName("column2").setType("Integer")));
    Table testTable = new Table().setTableReference(ref).setSchema(schema);

    when(response.getStatusCode()).thenReturn(409); // 409 means already exists

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    Table ret =
        services.tryCreateTable(
            testTable, new RetryBoundedBackOff(0, BackOff.ZERO_BACKOFF), Sleeper.DEFAULT);

    assertNull(ret);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /** Tests that {@link BigQueryServicesImpl} retries quota rate limited attempts. */
  @Test
  public void testCreateTableRetry() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    Table testTable = new Table().setTableReference(ref);

    // First response is 403 rate limited, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(testTable));

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    Table ret =
        services.tryCreateTable(
            testTable, new RetryBoundedBackOff(3, BackOff.ZERO_BACKOFF), Sleeper.DEFAULT);
    assertEquals(testTable, ret);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    verifyNotNull(ret.getTableReference());
    expectedLogs.verifyInfo(
        "Quota limit reached when creating table project:dataset.table, "
            + "retrying up to 5.0 minutes");
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} uses the supplied {@link ErrorContainer}. */
  @Test
  public void testSimpleErrorRetrieval() throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("a", 1)), wrapValue(new TableRow().set("b", 2)));

    final TableDataInsertAllResponse failures =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors()
                        .setIndex(0L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout"))),
                    new InsertErrors()
                        .setIndex(1L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("invalid")))));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);

    when(response.getContent()).thenReturn(toStream(failures));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        failedInserts,
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false);

    assertThat(failedInserts, is(rows));
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} uses the supplied {@link ErrorContainer}. */
  @Test
  public void testExtendedErrorRetrieval() throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<ValueInSingleWindow<TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("a", 1)), wrapValue(new TableRow().set("b", 2)));

    final TableDataInsertAllResponse failures =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors()
                        .setIndex(0L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("timeout"))),
                    new InsertErrors()
                        .setIndex(1L)
                        .setErrors(ImmutableList.of(new ErrorProto().setReason("invalid")))));

    final List<ValueInSingleWindow<BigQueryInsertError>> expected =
        ImmutableList.of(
            wrapValue(
                new BigQueryInsertError(
                    rows.get(0).getValue(), failures.getInsertErrors().get(0), ref)),
            wrapValue(
                new BigQueryInsertError(
                    rows.get(1).getValue(), failures.getInsertErrors().get(1), ref)));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);

    when(response.getContent()).thenReturn(toStream(failures));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    List<ValueInSingleWindow<BigQueryInsertError>> failedInserts = Lists.newArrayList();
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        failedInserts,
        ErrorContainer.BIG_QUERY_INSERT_ERROR_ERROR_CONTAINER,
        false,
        false,
        false);

    assertThat(failedInserts, is(expected));
  }
}
