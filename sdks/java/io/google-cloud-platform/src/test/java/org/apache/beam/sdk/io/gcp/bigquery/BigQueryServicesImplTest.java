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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
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
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.RetryBoundedBackOff;
import com.google.protobuf.Parser;
import com.google.rpc.RetryInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.DatasetServiceImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.JobServiceImpl;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.FastNanoClockAndSleeper;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.FailsafeValueInSingleWindow;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BigQueryServicesImpl}. */
@RunWith(JUnit4.class)
public class BigQueryServicesImplTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(BigQueryServicesImpl.class);
  // A test can make mock responses through setupMockResponses
  private LowLevelHttpResponse[] responses;

  private MockLowLevelHttpRequest request;
  private Bigquery bigquery;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Set up the MockHttpRequest for future inspection.
    request =
        new MockLowLevelHttpRequest() {
          int index = 0;

          @Override
          public LowLevelHttpResponse execute() throws IOException {
            Verify.verify(
                index < responses.length,
                "The number of HttpRequest invocation exceeded the number of prepared mock requests. Index: %s - Len is: %s",
                index,
                responses.length);
            return responses[index++];
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

    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
    MetricsEnvironment.setCurrentContainer(container);
  }

  @FunctionalInterface
  private interface MockSetupFunction {
    void apply(LowLevelHttpResponse t) throws IOException;
  }

  /**
   * Prepares the mock objects using {@code mockPreparations}, and assigns them to {@link
   * #responses}.
   */
  private void setupMockResponses(MockSetupFunction... mockPreparations) throws IOException {
    responses = new LowLevelHttpResponse[mockPreparations.length];
    for (int i = 0; i < mockPreparations.length; ++i) {
      MockSetupFunction setupFunction = mockPreparations[i];
      LowLevelHttpResponse response = mock(LowLevelHttpResponse.class);
      setupFunction.apply(response);
      responses[i] = response;
    }
  }

  /**
   * Verifies the test interacted the mock objects in {@link #responses}.
   *
   * <p>The implementation of google-api-client or google-http-client may influence the number of
   * interaction in future
   */
  private void verifyAllResponsesAreRead() throws IOException {
    Verify.verify(responses != null, "The test setup is incorrect. Responses are not setup");
    for (LowLevelHttpResponse response : responses) {
      // Google-http-client reads the field twice per response.
      verify(response, atLeastOnce()).getStatusCode();
      verify(response, times(1)).getContent();
      verify(response, times(1)).getContentType();
    }
  }

  private void verifyRequestMetricWasSet(
      String method, String projectId, String dataset, String table, String status, long count) {
    // Verify the metric as reported.
    HashMap<String, String> labels = new HashMap<String, String>();
    // TODO(ajamato): Add Ptransform label. Populate it as empty for now to prevent the
    // SpecMonitoringInfoValidator from dropping the MonitoringInfo.
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    labels.put(MonitoringInfoConstants.Labels.SERVICE, "BigQuery");
    labels.put(MonitoringInfoConstants.Labels.METHOD, method);
    labels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.bigQueryTable(projectId, dataset, table));
    labels.put(MonitoringInfoConstants.Labels.BIGQUERY_PROJECT_ID, projectId);
    labels.put(MonitoringInfoConstants.Labels.BIGQUERY_DATASET, dataset);
    labels.put(MonitoringInfoConstants.Labels.BIGQUERY_TABLE, table);
    labels.put(MonitoringInfoConstants.Labels.STATUS, status);

    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, labels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getProcessWideContainer();
    assertEquals(count, (long) container.getCounter(name).getCumulative());
  }

  private void verifyWriteMetricWasSet(
      String projectId, String dataset, String table, String status, long count) {
    verifyRequestMetricWasSet("BigQueryBatchWrite", projectId, dataset, table, status, count);
  }

  private void verifyReadMetricWasSet(
      String projectId, String dataset, String table, String status, long count) {
    verifyRequestMetricWasSet("BigQueryBatchRead", projectId, dataset, table, status, count);
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds. */
  @Test
  public void testStartLoadJobSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    JobReference jobRef = new JobReference();
    jobRef.setJobId("jobId");
    jobRef.setProjectId("projectId");
    testJob.setJobReference(jobRef);

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testJob));
        });

    Sleeper sleeper = new FastNanoClockAndSleeper()::sleep;
    JobServiceImpl.startJob(
        testJob,
        new ApiErrorExtractor(),
        bigquery,
        sleeper,
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff()));

    verifyAllResponsesAreRead();
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

    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(409); // 409 means already exists
        });

    Sleeper sleeper = new FastNanoClockAndSleeper()::sleep;
    JobServiceImpl.startJob(
        testJob,
        new ApiErrorExtractor(),
        bigquery,
        sleeper,
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff()));

    verifyAllResponsesAreRead();
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
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testJob));
        });

    Sleeper sleeper = new FastNanoClockAndSleeper()::sleep;
    JobServiceImpl.startJob(
        testJob,
        new ApiErrorExtractor(),
        bigquery,
        sleeper,
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff()));

    verifyAllResponsesAreRead();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} succeeds. */
  @Test
  public void testPollJobSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE"));

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testJob));
        });

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verifyAllResponsesAreRead();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} fails. */
  @Test
  public void testPollJobFailed() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE").setErrorResult(new ErrorProto()));

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testJob));
        });

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verifyAllResponsesAreRead();
  }

  /** Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} returns UNKNOWN. */
  @Test
  public void testPollJobUnknown() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus());

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testJob));
        });

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.STOP_BACKOFF);

    assertEquals(null, job);
    verifyAllResponsesAreRead();
  }

  @Test
  public void testGetJobSucceeds() throws Exception {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus());

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testJob));
        });

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.getJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verifyAllResponsesAreRead();
  }

  @Test
  public void testGetJobNotFound() throws Exception {
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(404);
        });

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference().setProjectId("projectId").setJobId("jobId");
    Job job = jobService.getJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(null, job);
    verifyAllResponsesAreRead();
  }

  @Test
  public void testGetJobThrows() throws Exception {
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(401);
        });

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

    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testTable));
        });

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());

    Table table =
        datasetService.getTable(
            tableRef, Collections.emptyList(), null, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT);

    assertEquals(testTable, table);
    verifyAllResponsesAreRead();
  }

  @Test
  public void testGetTableNullProjectSucceeds() throws Exception {
    TableReference tableRef =
        new TableReference().setProjectId(null).setDatasetId("datasetId").setTableId("tableId");

    Table testTable = new Table();
    testTable.setTableReference(tableRef.clone().setProjectId("projectId"));

    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testTable));
        });

    BigQueryOptions options = PipelineOptionsFactory.create().as(BigQueryOptions.class);
    options.setBigQueryProject("projectId");

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(bigquery, null, options);

    Table table =
        datasetService.getTable(
            tableRef, Collections.emptyList(), null, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT);

    assertEquals(testTable, table);
    verifyAllResponsesAreRead();
  }

  @Test
  public void testGetTableNotFound() throws IOException, InterruptedException {
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(404);
        });

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");
    Table table =
        datasetService.getTable(
            tableRef, Collections.emptyList(), null, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT);

    assertNull(table);
    verifyAllResponsesAreRead();
  }

  @Test
  public void testGetTableThrows() throws Exception {
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(401);
        });

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Unable to get table: %s", tableRef.getTableId()));

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());
    datasetService.getTable(
        tableRef, Collections.emptyList(), null, BackOff.STOP_BACKOFF, Sleeper.DEFAULT);
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
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testDataList));
        });

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());

    assertFalse(datasetService.isTableEmpty(tableRef, BackOff.ZERO_BACKOFF, Sleeper.DEFAULT));

    verifyAllResponsesAreRead();
  }

  @Test
  public void testIsTableEmptyNoRetryForNotFound() throws IOException, InterruptedException {
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(404);
        });

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());

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
      verifyAllResponsesAreRead();
    }
  }

  @Test
  public void testIsTableEmptyThrows() throws Exception {
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(401);
        });

    TableReference tableRef =
        new TableReference()
            .setProjectId("projectId")
            .setDatasetId("datasetId")
            .setTableId("tableId");

    BigQueryServicesImpl.DatasetServiceImpl datasetService =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Unable to list table data: %s", tableRef.getTableId()));

    datasetService.isTableEmpty(tableRef, BackOff.STOP_BACKOFF, Sleeper.DEFAULT);
  }

  @Test
  public void testExecuteWithRetries() throws IOException, InterruptedException {
    Table testTable = new Table();
    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testTable));
        });

    Table table =
        BigQueryServicesImpl.executeWithRetries(
            bigquery.tables().get("projectId", "datasetId", "tableId"),
            "Failed to get table.",
            Sleeper.DEFAULT,
            BackOff.STOP_BACKOFF,
            BigQueryServicesImpl.ALWAYS_RETRY);

    assertEquals(testTable, table);
    verifyAllResponsesAreRead();
  }

  private <T> FailsafeValueInSingleWindow<T, T> wrapValue(T value) {
    return FailsafeValueInSingleWindow.of(
        value,
        GlobalWindow.TIMESTAMP_MAX_VALUE,
        GlobalWindow.INSTANCE,
        PaneInfo.ON_TIME_AND_ONLY_FIRING,
        value);
  }

  private <T> ValueInSingleWindow<T> wrapErrorValue(T value) {
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
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    // First response is 403 rate limited, second response has valid payload.
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(new TableDataInsertAllResponse()));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false,
        null);

    verifyAllResponsesAreRead();
    expectedLogs.verifyInfo("BigQuery insertAll error, retrying:");

    verifyWriteMetricWasSet("project", "dataset", "table", "ratelimitexceeded", 1);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} retries quota exceeded attempts. */
  @Test
  public void testInsertQuotaExceededRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    // First response is 403 quota exceeded, second response has valid payload.
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("quotaExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(new TableDataInsertAllResponse()));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false,
        null);

    verifyAllResponsesAreRead();
    expectedLogs.verifyInfo("BigQuery insertAll error, retrying:");

    verifyWriteMetricWasSet("project", "dataset", "table", "quotaexceeded", 1);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} can stop quotaExceeded retry attempts. */
  @Test
  public void testInsertStoppedRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    MockSetupFunction quotaExceededResponse =
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("quotaExceeded", 403)));
        };

    // Respond 403 four times, then valid payload.
    setupMockResponses(
        quotaExceededResponse,
        quotaExceededResponse,
        quotaExceededResponse,
        quotaExceededResponse,
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(new TableDataInsertAllResponse()));
        });

    thrown.expect(RuntimeException.class);

    // Google-http-client 1.39.1 and higher does not read the content of the response with error
    // status code. How can we ensure appropriate exception is thrown?
    thrown.expectMessage("quotaExceeded");

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false,
        null);

    verifyAllResponsesAreRead();

    verifyWriteMetricWasSet("project", "dataset", "table", "quotaexceeded", 1);
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
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("row", "a")), wrapValue(new TableRow().set("row", "b")));
    List<String> insertIds = ImmutableList.of("a", "b");

    final TableDataInsertAllResponse bFailed =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors().setIndex(1L).setErrors(ImmutableList.of(new ErrorProto()))));

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(bFailed));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());
    dataService.insertAll(
        ref,
        rows,
        insertIds,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false,
        null);

    verifyAllResponsesAreRead();

    verifyWriteMetricWasSet("project", "dataset", "table", "unknown", 1);
    verifyWriteMetricWasSet("project", "dataset", "table", "ok", 1);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} does not go over limit of rows per request. */
  @Test
  public void testInsertWithinRowCountLimits() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("tablercl");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("row", "a")),
            wrapValue(new TableRow().set("row", "b")),
            wrapValue(new TableRow().set("row", "c")));
    List<String> insertIds = ImmutableList.of("a", "b", "c");

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        });

    PipelineOptions options =
        PipelineOptionsFactory.fromArgs("--maxStreamingRowsToBatch=1").create();
    options.as(GcsOptions.class).setExecutorService(Executors.newSingleThreadExecutor());

    DatasetServiceImpl dataService = new DatasetServiceImpl(bigquery, null, options);
    dataService.insertAll(
        ref,
        rows,
        insertIds,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false,
        null);

    verifyAllResponsesAreRead();

    verifyWriteMetricWasSet("project", "dataset", "tablercl", "ok", 3);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} does not go over limit of rows per request. */
  @SuppressWarnings("InlineMeInliner") // inline `Strings.repeat()` - Java 11+ API only
  @Test
  public void testInsertWithinRequestByteSizeLimitsErrorsOut() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("tablersl");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("row", Strings.repeat("abcdefghi", 1024 * 1025))),
            wrapValue(new TableRow().set("row", "a")),
            wrapValue(new TableRow().set("row", "b")));
    List<String> insertIds = ImmutableList.of("a", "b", "c");

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.fromArgs("--maxStreamingBatchSize=15").create());
    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    List<ValueInSingleWindow<TableRow>> successfulRows = Lists.newArrayList();
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                dataService.<TableRow>insertAll(
                    ref,
                    rows,
                    insertIds,
                    BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
                    TEST_BACKOFF,
                    new MockSleeper(),
                    InsertRetryPolicy.alwaysRetry(),
                    failedInserts,
                    ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
                    false,
                    false,
                    false,
                    successfulRows));

    assertThat(e.getMessage(), containsString("exceeded BigQueryIO limit of 9MB."));
  }

  @SuppressWarnings("InlineMeInliner") // inline `Strings.repeat()` - Java 11+ API only
  @Test
  public void testInsertRetryTransientsAboveRequestByteSizeLimits() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("row", Strings.repeat("abcdefghi", 1024 * 1025))),
            wrapValue(new TableRow().set("row", "a")),
            wrapValue(new TableRow().set("row", "b")));
    List<String> insertIds = ImmutableList.of("a", "b", "c");

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.fromArgs("--maxStreamingBatchSize=15").create());
    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    List<ValueInSingleWindow<TableRow>> successfulRows = Lists.newArrayList();
    dataService.<TableRow>insertAll(
        ref,
        rows,
        insertIds,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.retryTransientErrors(),
        failedInserts,
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false,
        successfulRows);

    assertEquals(1, failedInserts.size());
    assertEquals(2, successfulRows.size());

    verifyAllResponsesAreRead();
    verifyWriteMetricWasSet("project", "dataset", "table", "ok", 1);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} does not go over limit of rows per request. */
  @Test
  public void testInsertWithinRequestByteSizeLimits() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("tablebsl");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("row", "a")),
            wrapValue(new TableRow().set("row", "b")),
            wrapValue(new TableRow().set("row", "cdefghijklmnopqrstuvwxyz")));
    List<String> insertIds = ImmutableList.of("a", "b", "c");

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        });

    PipelineOptions options =
        PipelineOptionsFactory.fromArgs("--maxStreamingBatchSize=15").create();
    options.as(GcsOptions.class).setExecutorService(Executors.newSingleThreadExecutor());

    DatasetServiceImpl dataService = new DatasetServiceImpl(bigquery, null, options);
    dataService.insertAll(
        ref,
        rows,
        insertIds,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.retryTransientErrors(),
        new ArrayList<>(),
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false,
        null);

    verifyAllResponsesAreRead();

    verifyWriteMetricWasSet("project", "dataset", "tablebsl", "ok", 2);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} fails gracefully when persistent issues. */
  @Test
  public void testInsertFailsGracefully() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(wrapValue(new TableRow()), wrapValue(new TableRow()));

    ErrorProto errorProto = new ErrorProto().setReason("schemaMismatch");
    final TableDataInsertAllResponse row1Failed =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors().setIndex(1L).setErrors(ImmutableList.of(errorProto))));

    final TableDataInsertAllResponse row0Failed =
        new TableDataInsertAllResponse()
            .setInsertErrors(
                ImmutableList.of(
                    new InsertErrors().setIndex(0L).setErrors(ImmutableList.of(errorProto))));

    MockSetupFunction row0FailureResponseFunction =
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          // Always return 200.
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenAnswer(invocation -> toStream(row0Failed));
        };

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          // Always return 200.
          when(response.getStatusCode()).thenReturn(200);
          // Return row 1 failing, then we retry row 1 as row 0, and row 0 persistently fails.
          when(response.getContent()).thenReturn(toStream(row1Failed));
        },
        // 3 failures
        row0FailureResponseFunction,
        row0FailureResponseFunction,
        row0FailureResponseFunction);

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());

    // Expect it to fail.
    try {
      dataService.insertAll(
          ref,
          rows,
          null,
          BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
          TEST_BACKOFF,
          new MockSleeper(),
          InsertRetryPolicy.alwaysRetry(),
          null,
          null,
          false,
          false,
          false,
          null);
      fail();
    } catch (IOException e) {
      assertThat(e, instanceOf(IOException.class));
      assertThat(e.getMessage(), containsString("Insert failed:"));
      assertThat(e.getMessage(), containsString("[{\"errors\":[{\"reason\":\"schemaMismatch\"}]"));
    }

    // Verify the exact number of retries as well as log messages.
    verifyAllResponsesAreRead();
    expectedLogs.verifyInfo("Retrying 1 failed inserts to BigQuery");

    verifyWriteMetricWasSet("project", "dataset", "table", "schemamismatch", 4);
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} will not retry other non-rate-limited,
   * non-quota-exceeded attempts.
   */
  @Test
  public void testFailInsertOtherRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    // First response is 403 non-{rate-limited, quota-exceeded}, second response has valid payload
    // but should not be invoked.
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("actually forbidden", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(new TableDataInsertAllResponse()));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("actually forbidden");
    try {
      dataService.insertAll(
          ref,
          rows,
          null,
          BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
          TEST_BACKOFF,
          new MockSleeper(),
          InsertRetryPolicy.alwaysRetry(),
          null,
          null,
          false,
          false,
          false,
          null);
    } finally {
      verify(responses[0], atLeastOnce()).getStatusCode();
      verify(responses[0]).getContent();
      verify(responses[0]).getContentType();
      // It should not invoke 2nd response
      verify(responses[1], never()).getStatusCode();
      verify(responses[1], never()).getContent();
      verify(responses[1], never()).getContentType();
    }

    verifyWriteMetricWasSet("project", "dataset", "table", "actually forbidden", 1);
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} logs suggested remedy for insert timeouts. */
  @Test
  public void testInsertTimeoutLog() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows = new ArrayList<>();
    rows.add(wrapValue(new TableRow()));

    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(400);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(
                  toStream(errorWithReasonAndStatus(" No rows present in the request. ", 400)));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                dataService.insertAll(
                    ref,
                    rows,
                    null,
                    BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
                    TEST_BACKOFF,
                    new MockSleeper(),
                    InsertRetryPolicy.alwaysRetry(),
                    null,
                    null,
                    false,
                    false,
                    false,
                    null));

    assertThat(e.getCause().getMessage(), containsString("No rows present in the request."));

    verifyAllResponsesAreRead();
    expectedLogs.verifyError("No rows present in the request error likely caused by");

    verifyWriteMetricWasSet("project", "dataset", "table", " no rows present in the request. ", 1);
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} uses the supplied {@link InsertRetryPolicy},
   * and returns the list of rows not retried.
   */
  @Test
  public void testInsertRetryPolicy() throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
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

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          // Always return 200.
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(firstFailure));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(secondFialure));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());

    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.retryTransientErrors(),
        failedInserts,
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false,
        null);
    assertEquals(1, failedInserts.size());
    expectedLogs.verifyInfo("Retrying 1 failed inserts to BigQuery");

    verifyWriteMetricWasSet("project", "dataset", "table", "timeout", 2);
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
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(wrapValue(new TableRow()), wrapValue(new TableRow()));

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    // Return a 200 response each time
    MockSetupFunction allRowsSucceededResponseFunction =
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(allRowsSucceeded));
        };
    setupMockResponses(allRowsSucceededResponseFunction, allRowsSucceededResponseFunction);

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());

    // First, test with all flags disabled
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        Lists.newArrayList(),
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false,
        null);

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
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        Lists.newArrayList(),
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        true,
        true,
        true,
        null);

    parsedRequest = fromString(request.getContentAsString(), TableDataInsertAllRequest.class);

    assertTrue(parsedRequest.getSkipInvalidRows());
    assertTrue(parsedRequest.getIgnoreUnknownValues());
    assertNull(parsedRequest.getRows().get(0).getInsertId());
    assertNull(parsedRequest.getRows().get(1).getInsertId());

    verifyWriteMetricWasSet("project", "dataset", "table", "ok", 2);
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
    HttpResponseException.Builder builder = mock(HttpResponseException.Builder.class);

    ErrorInfo info = new ErrorInfo();
    info.setReason("QuotaExceeded");
    List<ErrorInfo> infoList = new ArrayList<>();
    infoList.add(info);
    GoogleJsonError error = new GoogleJsonError();
    error.setErrors(infoList);
    IOException validException = new GoogleJsonResponseException(builder, error);

    IOException invalidException = new IOException();
    IOException nullDetailsException = new GoogleJsonResponseException(builder, null);
    IOException nullErrorsException =
        new GoogleJsonResponseException(builder, new GoogleJsonError());

    assertEquals(info.getReason(), DatasetServiceImpl.getErrorInfo(validException).getReason());
    assertNull(DatasetServiceImpl.getErrorInfo(invalidException));
    assertNull(DatasetServiceImpl.getErrorInfo(nullDetailsException));
    assertNull(DatasetServiceImpl.getErrorInfo(nullErrorsException));
  }

  @Test
  public void testCreateTableSucceeds() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    Table testTable = new Table().setTableReference(ref);

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testTable));
        });

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());
    Table ret =
        services.tryCreateTable(
            testTable, new RetryBoundedBackOff(BackOff.ZERO_BACKOFF, 0), Sleeper.DEFAULT);
    assertEquals(testTable, ret);
    verifyAllResponsesAreRead();
  }

  /** Tests that {@link BigQueryServicesImpl} does not retry non-rate-limited attempts. */
  @Test
  public void testCreateTableDoesNotRetry() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    Table testTable = new Table().setTableReference(ref);
    // First response is 403 not-rate-limited, second response has valid payload but should not
    // be invoked.
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("actually forbidden", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testTable));
        });

    thrown.expect(GoogleJsonResponseException.class);
    thrown.expectMessage("actually forbidden");

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());
    try {
      services.tryCreateTable(
          testTable, new RetryBoundedBackOff(BackOff.ZERO_BACKOFF, 3), Sleeper.DEFAULT);
      fail();
    } catch (IOException e) {
      verify(responses[0], atLeastOnce()).getStatusCode();
      verify(responses[0]).getContent();
      verify(responses[0]).getContentType();
      // It should not invoke 2nd response
      verify(responses[1], never()).getStatusCode();
      verify(responses[1], never()).getContent();
      verify(responses[1], never()).getContentType();
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

    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(409); // 409 means already exists
        });

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());
    Table ret =
        services.tryCreateTable(
            testTable, new RetryBoundedBackOff(BackOff.ZERO_BACKOFF, 0), Sleeper.DEFAULT);

    assertNull(ret);
    verifyAllResponsesAreRead();
  }

  /** Tests that {@link BigQueryServicesImpl} retries quota rate limited attempts. */
  @Test
  public void testCreateTableRetry() throws IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    Table testTable = new Table().setTableReference(ref);

    // First response is 403 rate limited, second response has valid payload.
    setupMockResponses(
        response -> {
          when(response.getStatusCode()).thenReturn(403);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getContent())
              .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)));
        },
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(testTable));
        });

    BigQueryServicesImpl.DatasetServiceImpl services =
        new BigQueryServicesImpl.DatasetServiceImpl(
            bigquery, null, PipelineOptionsFactory.create());
    Table ret =
        services.tryCreateTable(
            testTable, new RetryBoundedBackOff(BackOff.ZERO_BACKOFF, 3), Sleeper.DEFAULT);
    assertEquals(testTable, ret);
    verifyAllResponsesAreRead();

    assertNotNull(ret.getTableReference());

    expectedLogs.verifyInfo(
        "Quota limit reached when creating table project:dataset.table, "
            + "retrying up to 5 minutes");
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} uses the supplied {@link ErrorContainer}. */
  @Test
  public void testSimpleErrorRetrieval() throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
        ImmutableList.of(
            wrapValue(new TableRow().set("a", 1)), wrapValue(new TableRow().set("b", 2)));

    final List<ValueInSingleWindow<TableRow>> expected =
        ImmutableList.of(
            wrapErrorValue(new TableRow().set("a", 1)), wrapErrorValue(new TableRow().set("b", 2)));

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

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContent()).thenReturn(toStream(failures));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());

    List<ValueInSingleWindow<TableRow>> failedInserts = Lists.newArrayList();
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        failedInserts,
        ErrorContainer.TABLE_ROW_ERROR_CONTAINER,
        false,
        false,
        false,
        null);

    assertThat(failedInserts, is(expected));
  }

  /** Tests that {@link DatasetServiceImpl#insertAll} uses the supplied {@link ErrorContainer}. */
  @Test
  public void testExtendedErrorRetrieval() throws InterruptedException, IOException {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> rows =
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
            wrapErrorValue(
                new BigQueryInsertError(
                    rows.get(0).getValue(), failures.getInsertErrors().get(0), ref)),
            wrapErrorValue(
                new BigQueryInsertError(
                    rows.get(1).getValue(), failures.getInsertErrors().get(1), ref)));

    setupMockResponses(
        response -> {
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
          when(response.getStatusCode()).thenReturn(200);
          when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);

          when(response.getContent()).thenReturn(toStream(failures));
        });

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, null, PipelineOptionsFactory.create());

    List<ValueInSingleWindow<BigQueryInsertError>> failedInserts = Lists.newArrayList();
    dataService.insertAll(
        ref,
        rows,
        null,
        BackOffAdapter.toGcpBackOff(TEST_BACKOFF.backoff()),
        TEST_BACKOFF,
        new MockSleeper(),
        InsertRetryPolicy.neverRetry(),
        failedInserts,
        ErrorContainer.BIG_QUERY_INSERT_ERROR_ERROR_CONTAINER,
        false,
        false,
        false,
        null);

    assertThat(failedInserts, is(expected));
  }

  @Test
  public void testCreateReadSessionSetsRequestCountMetric()
      throws InterruptedException, IOException {
    BigQueryServicesImpl.StorageClientImpl client =
        mock(BigQueryServicesImpl.StorageClientImpl.class);

    CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder();
    builder.getReadSessionBuilder().setTable("myproject:mydataset.mytable");
    CreateReadSessionRequest request = builder.build();
    when(client.callCreateReadSession(request))
        .thenReturn(ReadSession.newBuilder().build()); // Mock implementation.
    when(client.createReadSession(any())).thenCallRealMethod(); // Real implementation.

    client.createReadSession(request);
    verifyReadMetricWasSet("myproject", "mydataset", "mytable", "ok", 1);
  }

  @Test
  public void testCreateReadSessionSetsRequestCountMetricOnError()
      throws InterruptedException, IOException {
    BigQueryServicesImpl.StorageClientImpl client =
        mock(BigQueryServicesImpl.StorageClientImpl.class);

    CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder();
    builder.getReadSessionBuilder().setTable("myproject:mydataset.mytable");
    CreateReadSessionRequest request = builder.build();
    StatusCode statusCode =
        new StatusCode() {
          @Override
          public Code getCode() {
            return Code.NOT_FOUND;
          }

          @Override
          public Object getTransportCode() {
            return null;
          }
        };
    when(client.callCreateReadSession(request))
        .thenThrow(new ApiException("Not Found", null, statusCode, false)); // Mock implementation.
    when(client.createReadSession(any())).thenCallRealMethod(); // Real implementation.

    thrown.expect(ApiException.class);
    thrown.expectMessage("Not Found");

    client.createReadSession(request);
    verifyReadMetricWasSet("myproject", "mydataset", "mytable", "not_found", 1);
  }

  @Test
  public void testReadRowsSetsRequestCountMetric() throws InterruptedException, IOException {
    BigQueryServices.StorageClient client = mock(BigQueryServicesImpl.StorageClientImpl.class);
    ReadRowsRequest request = null;
    BigQueryServices.BigQueryServerStream<ReadRowsResponse> response =
        new BigQueryServices.BigQueryServerStream<ReadRowsResponse>() {
          @Override
          public Iterator<ReadRowsResponse> iterator() {
            return null;
          }

          @Override
          public void cancel() {}
        };

    when(client.readRows(request)).thenReturn(response); // Mock implementation.
    when(client.readRows(any(), any())).thenCallRealMethod(); // Real implementation.

    client.readRows(request, "myproject:mydataset.mytable");
    verifyReadMetricWasSet("myproject", "mydataset", "mytable", "ok", 1);
  }

  @Test
  public void testReadRowsSetsRequestCountMetricOnError() throws InterruptedException, IOException {
    BigQueryServices.StorageClient client = mock(BigQueryServicesImpl.StorageClientImpl.class);
    ReadRowsRequest request = null;
    StatusCode statusCode =
        new StatusCode() {
          @Override
          public Code getCode() {
            return Code.INTERNAL;
          }

          @Override
          public Object getTransportCode() {
            return null;
          }
        };
    when(client.readRows(request))
        .thenThrow(new ApiException("Internal", null, statusCode, false)); // Mock implementation.
    when(client.readRows(any(), any())).thenCallRealMethod(); // Real implementation.

    thrown.expect(ApiException.class);
    thrown.expectMessage("Internal");

    client.readRows(request, "myproject:mydataset.mytable");
    verifyReadMetricWasSet("myproject", "mydataset", "mytable", "internal", 1);
  }

  @Test
  public void testSplitReadStreamSetsRequestCountMetric() throws InterruptedException, IOException {
    BigQueryServices.StorageClient client = mock(BigQueryServicesImpl.StorageClientImpl.class);

    SplitReadStreamRequest request = null;
    when(client.splitReadStream(request))
        .thenReturn(SplitReadStreamResponse.newBuilder().build()); // Mock implementation.
    when(client.splitReadStream(any(), any())).thenCallRealMethod(); // Real implementation.

    client.splitReadStream(request, "myproject:mydataset.mytable");
    verifyReadMetricWasSet("myproject", "mydataset", "mytable", "ok", 1);
  }

  @Test
  public void testSplitReadStreamSetsRequestCountMetricOnError()
      throws InterruptedException, IOException {
    BigQueryServices.StorageClient client = mock(BigQueryServicesImpl.StorageClientImpl.class);
    SplitReadStreamRequest request = null;
    StatusCode statusCode =
        new StatusCode() {
          @Override
          public Code getCode() {
            return Code.RESOURCE_EXHAUSTED;
          }

          @Override
          public Object getTransportCode() {
            return null;
          }
        };
    when(client.splitReadStream(request))
        .thenThrow(
            new ApiException(
                "Resource Exhausted", null, statusCode, false)); // Mock implementation.
    when(client.splitReadStream(any(), any())).thenCallRealMethod(); // Real implementation.

    thrown.expect(ApiException.class);
    thrown.expectMessage("Resource Exhausted");

    client.splitReadStream(request, "myproject:mydataset.mytable");
    verifyReadMetricWasSet("myproject", "mydataset", "mytable", "resource_exhausted", 1);
  }

  @Test
  public void testRetryAttemptCounter() {
    BigQueryServicesImpl.StorageClientImpl.RetryAttemptCounter counter =
        new BigQueryServicesImpl.StorageClientImpl.RetryAttemptCounter();

    RetryInfo retryInfo =
        RetryInfo.newBuilder()
            .setRetryDelay(
                com.google.protobuf.Duration.newBuilder()
                    .setSeconds(123)
                    .setNanos(456000000)
                    .build())
            .build();

    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of(
            "google.rpc.retryinfo-bin",
            new Metadata.BinaryMarshaller<RetryInfo>() {
              @Override
              public byte[] toBytes(RetryInfo value) {
                return value.toByteArray();
              }

              @Override
              public RetryInfo parseBytes(byte[] serialized) {
                try {
                  Parser<RetryInfo> parser = RetryInfo.newBuilder().build().getParserForType();
                  return parser.parseFrom(serialized);
                } catch (Exception e) {
                  return null;
                }
              }
            }),
        retryInfo);

    MetricName metricName =
        MetricName.named(
            "org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$StorageClientImpl",
            "throttling-msecs");
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getCurrentContainer();

    // Nulls don't bump the counter.
    counter.onRetryAttempt(null, null);
    assertEquals(0, (long) container.getCounter(metricName).getCumulative());

    // Resource exhausted with empty metadata doesn't bump the counter.
    counter.onRetryAttempt(
        Status.RESOURCE_EXHAUSTED.withDescription("You have consumed some quota"), new Metadata());
    assertEquals(0, (long) container.getCounter(metricName).getCumulative());

    // Resource exhausted with retry info bumps the counter.
    counter.onRetryAttempt(Status.RESOURCE_EXHAUSTED.withDescription("Stop for a while"), metadata);
    assertEquals(123456, (long) container.getCounter(metricName).getCumulative());

    // Other errors with retry info doesn't bump the counter.
    counter.onRetryAttempt(Status.UNAVAILABLE.withDescription("Server is gone"), metadata);
    assertEquals(123456, (long) container.getCounter(metricName).getCumulative());
  }
}
