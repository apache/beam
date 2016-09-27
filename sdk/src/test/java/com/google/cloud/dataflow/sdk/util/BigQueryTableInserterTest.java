/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static com.google.common.base.Verify.verifyNotNull;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import com.google.api.client.testing.util.MockSleeper;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.hadoop.util.RetryBoundedBackOff;
import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests of {@link BigQueryTableInserter}.
 */
@RunWith(JUnit4.class)
public class BigQueryTableInserterTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(BigQueryTableInserter.class);
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

  @After
  public void tearDown() throws IOException {
    // These three interactions happen for every request in the normal response parsing.
    verify(response, atLeastOnce()).getContentEncoding();
    verify(response, atLeastOnce()).getHeaderCount();
    verify(response, atLeastOnce()).getReasonPhrase();
    verifyNoMoreInteractions(response);
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

  /**
   * Tests that {@link BigQueryTableInserter} succeeds on the first try.
   */
  @Test
  public void testCreateTableSucceeds() throws IOException {
    Table testTable = new Table().setDescription("a table");

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getContent()).thenReturn(toStream(testTable));

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);
    Table ret =
        inserter.tryCreateTable(
            new Table(),
            "project",
            "dataset",
            new RetryBoundedBackOff(0, BackOff.ZERO_BACKOFF),
            Sleeper.DEFAULT);
    assertEquals(testTable, ret);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryTableInserter} succeeds when the table already exists.
   */
  @Test
  public void testCreateTableSucceedsAlreadyExists() throws IOException {
    when(response.getStatusCode()).thenReturn(409); // 409 means already exists

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);
    Table ret =
        inserter.tryCreateTable(
            new Table(),
            "project",
            "dataset",
            new RetryBoundedBackOff(0, BackOff.ZERO_BACKOFF),
            Sleeper.DEFAULT);

    assertNull(ret);
    verify(response, times(1)).getStatusCode();
    verify(response, times(1)).getContent();
    verify(response, times(1)).getContentType();
  }

  /**
   * Tests that {@link BigQueryTableInserter} retries quota rate limited attempts.
   */
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

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);
    Table ret =
        inserter.tryCreateTable(
            testTable,
            "project",
            "dataset",
            new RetryBoundedBackOff(3, BackOff.ZERO_BACKOFF),
            Sleeper.DEFAULT);
    assertEquals(testTable, ret);
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    verifyNotNull(ret.getTableReference());
    expectedLogs.verifyInfo(
        "Quota limit reached when creating table project:dataset.table, "
            + "retrying up to 5.0 minutes");
  }

  /**
   * Tests that {@link BigQueryTableInserter} does not retry non-rate-limited attempts.
   */
  @Test
  public void testCreateTableDoesNotRetry() throws IOException {
    Table testTable = new Table().setDescription("a table");

    // First response is 403 not-rate-limited, second response has valid payload but should not
    // be invoked.
    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(403).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(errorWithReasonAndStatus("actually forbidden", 403)))
        .thenReturn(toStream(testTable));

    thrown.expect(GoogleJsonResponseException.class);
    thrown.expectMessage("actually forbidden");

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);
    try {
      inserter.tryCreateTable(
          new Table(),
          "project",
          "dataset",
          new RetryBoundedBackOff(3, BackOff.ZERO_BACKOFF),
          Sleeper.DEFAULT);
      fail();
    } catch (IOException e) {
      verify(response, times(1)).getStatusCode();
      verify(response, times(1)).getContent();
      verify(response, times(1)).getContentType();
      throw e;
    }
  }

  /**
   * Tests that {@link BigQueryTableInserter#insertAll} retries quota rate limited attempts.
   */
  @Test
  public void testInsertRetry() throws IOException {
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

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);

    inserter.insertAll(ref, rows, null, null, TEST_BACKOFF.backoff(), new MockSleeper());
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    expectedLogs.verifyInfo("BigQuery insertAll exceeded rate limit, retrying");
  }

  /**
   * Tests that {@link BigQueryTableInserter#insertAll} retries selected rows on failure.
   */
  @Test
  public void testInsertRetrySelectRows() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<TableRow> rows = ImmutableList.of(
        new TableRow().set("row", "a"), new TableRow().set("row", "b"));
    List<String> insertIds = ImmutableList.of("a", "b");

    final TableDataInsertAllResponse bFailed = new TableDataInsertAllResponse()
        .setInsertErrors(ImmutableList.of(new InsertErrors().setIndex(1L)));

    final TableDataInsertAllResponse allRowsSucceeded = new TableDataInsertAllResponse();

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    when(response.getStatusCode()).thenReturn(200).thenReturn(200);
    when(response.getContent())
        .thenReturn(toStream(bFailed)).thenReturn(toStream(allRowsSucceeded));

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);
    inserter.insertAll(ref, rows, insertIds, null, TEST_BACKOFF.backoff(), new MockSleeper());
    verify(response, times(2)).getStatusCode();
    verify(response, times(2)).getContent();
    verify(response, times(2)).getContentType();
    expectedLogs.verifyInfo("Retrying 1 failed inserts to BigQuery");
  }

  // A BackOff that makes a total of 4 attempts
  private static final FluentBackoff TEST_BACKOFF = FluentBackoff.DEFAULT.withMaxRetries(3);

  /**
   * Tests that {@link BigQueryTableInserter#insertAll} fails gracefully when persistent issues.
   */
  @Test
  public void testInsertFailsGracefully() throws Exception {
    TableReference ref =
        new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("table");
    List<TableRow> rows = ImmutableList.of(new TableRow(), new TableRow());

    final TableDataInsertAllResponse row1Failed = new TableDataInsertAllResponse()
        .setInsertErrors(ImmutableList.of(new InsertErrors().setIndex(1L)));

    final TableDataInsertAllResponse row0Failed = new TableDataInsertAllResponse()
        .setInsertErrors(ImmutableList.of(new InsertErrors().setIndex(0L)));

    when(response.getContentType()).thenReturn(Json.MEDIA_TYPE);
    // Always return 200.
    when(response.getStatusCode()).thenReturn(200);
    // Return row 1 failing, then we retry row 1 as row 0, and row 0 persistently fails.
    when(response.getContent())
        .thenReturn(toStream(row1Failed))
        .thenAnswer(new Answer<InputStream>() {
          @Override
          public InputStream answer(InvocationOnMock invocation) throws Throwable {
            return toStream(row0Failed);
          }
        });

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);

    // Expect it to fail.
    try {
      inserter.insertAll(ref, rows, null, null, TEST_BACKOFF.backoff(), new MockSleeper());
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
   * Tests that {@link BigQueryTableInserter#insertAll} does not retry non-rate-limited attempts.
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

    BigQueryTableInserter inserter = new BigQueryTableInserter(bigquery);

    try {
      inserter.insertAll(ref, rows, null, null, TEST_BACKOFF.backoff(), new MockSleeper());
      fail();
    } catch (RuntimeException e) {
      verify(response, times(1)).getStatusCode();
      verify(response, times(1)).getContent();
      verify(response, times(1)).getContentType();
      throw e.getCause();
    }
  }
}
