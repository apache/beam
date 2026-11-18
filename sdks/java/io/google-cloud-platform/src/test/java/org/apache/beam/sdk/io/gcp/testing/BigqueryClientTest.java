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
package org.apache.beam.sdk.io.gcp.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BigqueryClient}. */
@RunWith(JUnit4.class)
public class BigqueryClientTest {
  private final String projectId = "test-project";
  private final String query = "test-query";
  private BigqueryClient bqClient;

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private Bigquery mockBigqueryClient;
  @Mock private Bigquery.Jobs mockJobs;
  @Mock private Bigquery.Jobs.Query mockQuery;
  @Mock private Bigquery.Jobs.Insert mockJobInsert;
  @Mock private Bigquery.Jobs.GetQueryResults mockGetQueryResults;
  @Mock private Bigquery.Datasets mockDatasets;
  @Mock private Bigquery.Datasets.Insert mockDatasetInsert;
  @Mock private Bigquery.Datasets.Delete mockDatasetDelete;
  @Mock private Bigquery.Tables mockTables;
  @Mock private Bigquery.Tables.Insert mockTableInsert;
  private MockedStatic<BigqueryClient> mockStatic;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(mockBigqueryClient.jobs()).thenReturn(mockJobs);
    when(mockJobs.query(anyString(), any(QueryRequest.class))).thenReturn(mockQuery);
    mockStatic = Mockito.mockStatic(BigqueryClient.class);
    when(BigqueryClient.getNewBigqueryClient(anyString())).thenReturn(mockBigqueryClient);
    bqClient = spy(new BigqueryClient("test-app"));
  }

  @After
  public void tearDown() {
    mockStatic.close();
  }

  @Test
  public void testQueryWithRetriesWhenServiceFails() throws Exception {
    when(mockQuery.execute()).thenThrow(new IOException());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable to get BigQuery response after retrying");
    try {
      bqClient.queryWithRetries(query, projectId);
    } finally {
      verify(mockJobs, atLeast(BigqueryClient.MAX_QUERY_RETRIES))
          .query(eq(projectId), any(QueryRequest.class));
    }
  }

  @Test
  public void testQueryWithRetriesWhenQueryResponseNull() throws Exception {
    when(mockQuery.execute()).thenReturn(null);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable to get BigQuery response after retrying");
    try {
      bqClient.queryWithRetries(query, projectId);
    } finally {
      verify(mockJobs, atLeast(BigqueryClient.MAX_QUERY_RETRIES))
          .query(eq(projectId), any(QueryRequest.class));
    }
  }

  private static GoogleJsonResponseException jsonError(int statusCode) {
    return jsonError(statusCode, "error");
  }

  private static GoogleJsonResponseException jsonError(int statusCode, String reason) {
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setDomain("global");
    errorInfo.setReason(reason);
    GoogleJsonError details = new GoogleJsonError();
    details.setCode(statusCode);
    details.setErrors(Collections.singletonList(errorInfo));
    return new GoogleJsonResponseException(
        new HttpResponseException.Builder(statusCode, reason, new HttpHeaders()), details);
  }

  private void stubQueryUnflattened() throws IOException {
    when(mockBigqueryClient.datasets()).thenReturn(mockDatasets);
    when(mockDatasets.insert(anyString(), any(Dataset.class))).thenReturn(mockDatasetInsert);
    when(mockDatasetInsert.execute()).thenReturn(new Dataset());
    when(mockDatasets.delete(anyString(), anyString())).thenReturn(mockDatasetDelete);
    when(mockDatasetDelete.setDeleteContents(true)).thenReturn(mockDatasetDelete);
    when(mockBigqueryClient.tables()).thenReturn(mockTables);
    when(mockTables.insert(anyString(), anyString(), any(Table.class))).thenReturn(mockTableInsert);
    when(mockTableInsert.execute()).thenReturn(new Table());
    when(mockJobs.insert(anyString(), any(Job.class))).thenReturn(mockJobInsert);
    when(mockJobInsert.execute())
        .thenReturn(new Job().setJobReference(new JobReference().setJobId("job-id")));
    when(mockJobs.getQueryResults(anyString(), anyString())).thenReturn(mockGetQueryResults);
    when(mockGetQueryResults.setLocation(any())).thenReturn(mockGetQueryResults);
  }

  private static GetQueryResultsResponse completeResponse(List<TableRow> rows) {
    return new GetQueryResultsResponse().setJobComplete(true).setRows(rows);
  }

  @Test
  public void testQueryUnflattenedStartsNewJobAfterServerError() throws Exception {
    stubQueryUnflattened();
    List<TableRow> rows = Collections.singletonList(new TableRow().set("f0_", "1"));
    when(mockGetQueryResults.execute())
        .thenThrow(jsonError(503, "backendError"))
        .thenReturn(completeResponse(rows));

    assertEquals(rows, bqClient.queryUnflattened(query, projectId, false, true));
    verify(mockJobs, times(2)).insert(eq(projectId), any(Job.class));
    verify(mockDatasets).delete(eq(projectId), anyString());
  }

  @Test
  public void testQueryUnflattenedRetriesJobInsertError() throws Exception {
    stubQueryUnflattened();
    List<TableRow> rows = Collections.singletonList(new TableRow().set("f0_", "1"));
    when(mockJobInsert.execute())
        .thenThrow(jsonError(503, "backendError"))
        .thenReturn(new Job().setJobReference(new JobReference().setJobId("job-id")));
    when(mockGetQueryResults.execute()).thenReturn(completeResponse(rows));

    assertEquals(rows, bqClient.queryUnflattened(query, projectId, false, true));
    verify(mockJobInsert, times(2)).execute();
  }

  @Test
  public void testQueryUnflattenedRetriesRateLimitExceeded() throws Exception {
    stubQueryUnflattened();
    List<TableRow> rows = Collections.singletonList(new TableRow().set("f0_", "1"));
    when(mockGetQueryResults.execute())
        .thenThrow(jsonError(403, "rateLimitExceeded"))
        .thenReturn(completeResponse(rows));

    assertEquals(rows, bqClient.queryUnflattened(query, projectId, false, true));
    verify(mockGetQueryResults, times(2)).execute();
  }

  @Test
  public void testQueryUnflattenedDoesNotRetryClientError() throws Exception {
    stubQueryUnflattened();
    when(mockGetQueryResults.execute()).thenThrow(jsonError(400, "invalidQuery"));

    thrown.expect(GoogleJsonResponseException.class);
    try {
      bqClient.queryUnflattened(query, projectId, false, true);
    } finally {
      verify(mockGetQueryResults, times(1)).execute();
      verify(mockDatasets).delete(eq(projectId), anyString());
    }
  }

  @Test
  public void testQueryUnflattenedDoesNotRetryNonJsonClientError() throws Exception {
    stubQueryUnflattened();
    when(mockGetQueryResults.execute())
        .thenThrow(new HttpResponseException.Builder(404, "Not Found", new HttpHeaders()).build());

    thrown.expect(HttpResponseException.class);
    try {
      bqClient.queryUnflattened(query, projectId, false, true);
    } finally {
      verify(mockGetQueryResults, times(1)).execute();
    }
  }

  @Test
  public void testQueryUnflattenedDeletesDatasetWhenNoRows() throws Exception {
    stubQueryUnflattened();
    when(mockGetQueryResults.execute())
        .thenReturn(new GetQueryResultsResponse().setJobComplete(true));

    assertTrue(bqClient.queryUnflattened(query, projectId, false, true).isEmpty());
    verify(mockDatasets).delete(eq(projectId), anyString());
  }
}
