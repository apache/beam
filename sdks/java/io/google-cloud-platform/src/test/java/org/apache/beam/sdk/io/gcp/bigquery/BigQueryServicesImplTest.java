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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.DatasetServiceImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.JobServiceImpl;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.FastNanoClockAndSleeper;
import org.apache.beam.sdk.util.AttemptBoundedExponentialBackOff;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.util.Transport;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonErrorContainer;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link BigQueryServicesImpl}.
 */
@RunWith(JUnit4.class)
public class BigQueryServicesImplTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(BigQueryServicesImpl.class);
  @Mock private LowLevelHttpResponse response;
  private Bigquery bigquery;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // A mock transport that lets us mock the API responses.
    MockHttpTransport transport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpRequest(
                new MockLowLevelHttpRequest() {
                  @Override
                  public LowLevelHttpResponse execute() throws IOException {
                    return response;
                  }
                })
            .build();

    // A sample BigQuery API client that uses default JsonFactory and RetryHttpInitializer.
    bigquery =
        new Bigquery.Builder(
                transport, Transport.getJsonFactory(), new RetryHttpRequestInitializer())
            .build();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds.
   */
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
    BackOff backoff = new AttemptBoundedExponentialBackOff(
        5 /* attempts */, 1000 /* initialIntervalMillis */);
    JobServiceImpl.startJob(testJob, new ApiErrorExtractor(), bigquery, sleeper, backoff);

    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds
   * with an already exist job.
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
    BackOff backoff = new AttemptBoundedExponentialBackOff(
        5 /* attempts */, 1000 /* initialIntervalMillis */);
    JobServiceImpl.startJob(testJob, new ApiErrorExtractor(), bigquery, sleeper, backoff);

    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#startLoadJob} succeeds with a retry.
   */
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
    BackOff backoff = new AttemptBoundedExponentialBackOff(
        5 /* attempts */, 1000 /* initialIntervalMillis */);
    JobServiceImpl.startJob(testJob, new ApiErrorExtractor(), bigquery, sleeper, backoff);

    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} succeeds.
   */
  @Test
  public void testPollJobSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE"));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference()
        .setProjectId("projectId")
        .setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} fails.
   */
  @Test
  public void testPollJobFailed() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE").setErrorResult(new ErrorProto()));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference()
        .setProjectId("projectId")
        .setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(testJob, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.JobServiceImpl#pollJob} returns UNKNOWN.
   */
  @Test
  public void testPollJobUnknown() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus());

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.JobServiceImpl jobService =
        new BigQueryServicesImpl.JobServiceImpl(bigquery);
    JobReference jobRef = new JobReference()
        .setProjectId("projectId")
        .setJobId("jobId");
    Job job = jobService.pollJob(jobRef, Sleeper.DEFAULT, BackOff.STOP_BACKOFF);

    assertEquals(null, job);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  @Test
  public void testExecuteWithRetries() throws IOException, InterruptedException {
    Table testTable = new Table();

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testTable));

    Table table = BigQueryServicesImpl.executeWithRetries(
        bigquery.tables().get("projectId", "datasetId", "tableId"),
        "Failed to get table.",
        Sleeper.DEFAULT,
        BackOff.STOP_BACKOFF);

    assertEquals(testTable, table);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} retries quota rate limited attempts.
   */
  @Test
  public void testInsertRetry() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<TableRow> rows = new ArrayList<>();
    rows.add(new TableRow());

    // First response is 403 rate limited, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(new TableDataInsertAllResponse()));

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());
    dataService.insertAll(ref, rows, null);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    expectedLogs.verifyInfo("BigQuery insertAll exceeded rate limit, retrying");
  }

  /**
   * Tests that {@link DatasetServiceImpl#insertAll} does not retry non-rate-limited attempts.
   */
  @Test
  public void testInsertDoesNotRetry() throws Throwable {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<TableRow> rows = new ArrayList<>();
    rows.add(new TableRow());

    // First response is 403 not-rate-limited, second response has valid payload but should not
    // be invoked.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("actually forbidden", 403)))
        .thenReturn(toStream(new TableDataInsertAllResponse()));

    thrown.expect(GoogleJsonResponseException.class);
    thrown.expectMessage("actually forbidden");

    DatasetServiceImpl dataService =
        new DatasetServiceImpl(bigquery, PipelineOptionsFactory.create());

    try {
      dataService.insertAll(ref, rows, null);
      fail();
    } catch (RuntimeException e) {
      verify(response, times(1)).getStatusCode();
      verify(response, times(1)).getContent();
      verify(response, times(1)).getContentType();
      throw e.getCause();
    }
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
    // The actual JSON response is an error container.
    GoogleJsonErrorContainer container = new GoogleJsonErrorContainer();
    container.setError(error);
    return container;
  }
}
