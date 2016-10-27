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
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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

    try {
      assertThat(mockResult, matcher);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("Total number of rows are: 1"));
      assertThat(expected.getMessage(), containsString("abc"));
      verify(matcher).newBigqueryClient(eq(appName));
      verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
    }
  }

  @Test
  public void testBigqueryMatcherFailsWhenResponseIsNull() throws IOException {
    testMatcherFailsSinceInvalidQueryResponse(null);
  }

  @Test
  public void testBigqueryMatcherFailsWhenNullRowsInResponse() throws IOException {
    testMatcherFailsSinceInvalidQueryResponse(new QueryResponse());
  }

  @Test
  public void testBigqueryMatcherFailsWhenEmptyRowsInResponse() throws IOException {
    QueryResponse response = new QueryResponse();
    response.setRows(Lists.<TableRow>newArrayList());

    testMatcherFailsSinceInvalidQueryResponse(response);
  }

  private void testMatcherFailsSinceInvalidQueryResponse(QueryResponse response)
      throws IOException {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockQuery.execute()).thenReturn(response);

    try {
      assertThat(mockResult, matcher);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("Invalid BigQuery response:"));
      verify(matcher).newBigqueryClient(eq(appName));
      verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError is expected.");
  }

  @Test
  public void testQueryWithRetriesWhenServiceFails() throws Exception {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    when(mockQuery.execute()).thenThrow(new IOException());

    thrown.expect(IOException.class);
    thrown.expectMessage("Unable to get BigQuery response after retrying");

    matcher.queryWithRetries(
        mockBigqueryClient,
        new QueryRequest(),
        fastClock,
        BigqueryMatcher.BACKOFF_FACTORY.backoff());

    verify(matcher).newBigqueryClient(eq(appName));
    verify(mockJobs, times(BigqueryMatcher.MAX_QUERY_RETRIES))
        .query(eq(projectId), eq(new QueryRequest().setQuery(query)));
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
    response.setRows(Lists.newArrayList(row));
    response.setTotalRows(BigInteger.ONE);
    return response;
  }
}
