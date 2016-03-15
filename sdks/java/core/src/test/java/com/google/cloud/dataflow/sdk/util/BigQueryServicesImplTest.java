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
package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonErrorContainer;
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
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;
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
   * Tests that {@link BigQueryServicesImpl.LoadServiceImpl#startLoadJob} succeeds.
   */
  @Test
  public void testStartLoadJobSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    TableReference ref = new TableReference();
    ref.setProjectId("projectId");
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setDestinationTable(ref);

    Sleeper sleeper = new FastNanoClockAndSleeper();
    BackOff backoff = new AttemptBoundedExponentialBackOff(
        5 /* attempts */, 1000 /* initialIntervalMillis */);
    BigQueryServicesImpl.LoadServiceImpl loadService =
        new BigQueryServicesImpl.LoadServiceImpl(bigquery);
    loadService.startLoadJob("jobId", loadConfig, sleeper, backoff);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.LoadServiceImpl#startLoadJob} succeeds
   * with an already exist job.
   */
  @Test
  public void testStartLoadJobSucceedsAlreadyExists() throws IOException, InterruptedException {
    when(response.getStatusCode()).thenReturn(409); // 409 means already exists

    TableReference ref = new TableReference();
    ref.setProjectId("projectId");
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setDestinationTable(ref);

    Sleeper sleeper = new FastNanoClockAndSleeper();
    BackOff backoff = new AttemptBoundedExponentialBackOff(
        5 /* attempts */, 1000 /* initialIntervalMillis */);
    BigQueryServicesImpl.LoadServiceImpl loadService =
        new BigQueryServicesImpl.LoadServiceImpl(bigquery);
    loadService.startLoadJob("jobId", loadConfig, sleeper, backoff);

    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.LoadServiceImpl#startLoadJob} succeeds with a retry.
   */
  @Test
  public void testStartLoadJobRetry() throws IOException, InterruptedException {
    Job testJob = new Job();

    // First response is 403 rate limited, second response has valid payload.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("rateLimitExceeded", 403)))
        .thenReturn(toStream(testJob));

    TableReference ref = new TableReference();
    ref.setProjectId("projectId");
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setDestinationTable(ref);

    Sleeper sleeper = new FastNanoClockAndSleeper();
    BackOff backoff = new AttemptBoundedExponentialBackOff(
        5 /* attempts */, 1000 /* initialIntervalMillis */);
    BigQueryServicesImpl.LoadServiceImpl loadService =
        new BigQueryServicesImpl.LoadServiceImpl(bigquery);
    loadService.startLoadJob("jobId", loadConfig, sleeper, backoff);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.LoadServiceImpl#pollJobStatus} succeeds.
   */
  @Test
  public void testPollJobStatusSucceeds() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE"));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.LoadServiceImpl loadService =
        new BigQueryServicesImpl.LoadServiceImpl(bigquery);
    BigQueryServices.Status status =
        loadService.pollJobStatus("projectId", "jobId", Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(BigQueryServices.Status.SUCCEEDED, status);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.LoadServiceImpl#pollJobStatus} fails.
   */
  @Test
  public void testPollJobStatusFailed() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus().setState("DONE").setErrorResult(new ErrorProto()));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.LoadServiceImpl loadService =
        new BigQueryServicesImpl.LoadServiceImpl(bigquery);
    BigQueryServices.Status status =
        loadService.pollJobStatus("projectId", "jobId", Sleeper.DEFAULT, BackOff.ZERO_BACKOFF);

    assertEquals(BigQueryServices.Status.FAILED, status);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryServicesImpl.LoadServiceImpl#pollJobStatus} returns UNKNOWN.
   */
  @Test
  public void testPollJobStatusUnknown() throws IOException, InterruptedException {
    Job testJob = new Job();
    testJob.setStatus(new JobStatus());

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testJob));

    BigQueryServicesImpl.LoadServiceImpl loadService =
        new BigQueryServicesImpl.LoadServiceImpl(bigquery);
    BigQueryServices.Status status =
        loadService.pollJobStatus("projectId", "jobId", Sleeper.DEFAULT, BackOff.STOP_BACKOFF);

    assertEquals(BigQueryServices.Status.UNKNOWN, status);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
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

