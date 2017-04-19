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
package org.apache.beam.sdk.testing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.beam.sdk.PipelineResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link BigqueryMatcher}.
 */
@RunWith(JUnit4.class)
public class BigqueryMatcherTest {
  private final String appName = "test-app";
  private final String projectId = "test-project";
  private final String query = "test-query";

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public FastNanoClockAndSleeper fastClock = new FastNanoClockAndSleeper();
  @Mock private Bigquery mockBigqueryClient;
  @Mock private Bigquery.Jobs mockJobs;
  @Mock private Bigquery.Jobs.Query mockQuery;
  @Mock private PipelineResult mockResult;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(mockBigqueryClient.jobs()).thenReturn(mockJobs);
    when(mockJobs.query(anyString(), any(QueryRequest.class))).thenReturn(mockQuery);
  }

  @Test
  public void testBigqueryMatcherThatSucceeds() throws Exception {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(
            appName, projectId, query, "9bb47f5c90d2a99cad526453dff5ed5ec74650dc"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockQuery.execute()).thenReturn(createResponseContainingTestData());

    assertThat(mockResult, matcher);
    verify(matcher).newBigqueryClient(eq(appName));
    verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
  }

  @Test
  public void testBigqueryMatcherFailsForChecksumMismatch() throws IOException {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "incorrect-checksum"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockQuery.execute()).thenReturn(createResponseContainingTestData());

    thrown.expect(AssertionError.class);
    thrown.expectMessage("Total number of rows are: 1");
    thrown.expectMessage("abc");
    try {
      assertThat(mockResult, matcher);
    } finally {
      verify(matcher).newBigqueryClient(eq(appName));
      verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
    }
  }

  @Test
  public void testBigqueryMatcherFailsWhenQueryJobNotComplete() throws Exception {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockQuery.execute()).thenReturn(new QueryResponse().setJobComplete(false));

    thrown.expect(AssertionError.class);
    thrown.expectMessage("The query job hasn't completed.");
    thrown.expectMessage("jobComplete=false");
    try {
      assertThat(mockResult, matcher);
    } finally {
      verify(matcher).newBigqueryClient(eq(appName));
      verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
    }
  }

  @Test
  public void testQueryWithRetriesWhenServiceFails() throws Exception {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    when(mockQuery.execute()).thenThrow(new IOException());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable to get BigQuery response after retrying");
    try {
      matcher.queryWithRetries(
          mockBigqueryClient,
          new QueryRequest(),
          fastClock,
          BigqueryMatcher.BACKOFF_FACTORY.backoff());
    } finally {
      verify(mockJobs, atLeast(BigqueryMatcher.MAX_QUERY_RETRIES))
          .query(eq(projectId), eq(new QueryRequest()));
    }
  }

  @Test
  public void testQueryWithRetriesWhenQueryResponseNull() throws Exception {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    when(mockQuery.execute()).thenReturn(null);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Unable to get BigQuery response after retrying");
    try {
      matcher.queryWithRetries(
          mockBigqueryClient,
          new QueryRequest(),
          fastClock,
          BigqueryMatcher.BACKOFF_FACTORY.backoff());
    } finally {
      verify(mockJobs, atLeast(BigqueryMatcher.MAX_QUERY_RETRIES))
          .query(eq(projectId), eq(new QueryRequest()));
    }
  }

  private QueryResponse createResponseContainingTestData() {
    TableCell field1 = new TableCell();
    field1.setV("abc");
    TableCell field2 = new TableCell();
    field2.setV("2");
    TableCell field3 = new TableCell();
    field3.setV("testing BigQuery matcher.");
    TableRow row = new TableRow();
    row.setF(Lists.newArrayList(field1, field2, field3));

    QueryResponse response = new QueryResponse();
    response.setJobComplete(true);
    response.setRows(Lists.newArrayList(row));
    response.setTotalRows(BigInteger.ONE);
    return response;
  }
}
