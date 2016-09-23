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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import autovalue.shaded.com.google.common.common.collect.Lists;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.util.TestCredential;
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
  @Mock private Bigquery mockBigqueryClient;
  @Mock private Bigquery.Jobs mockJobs;
  @Mock private Bigquery.Jobs.Query mockQuery;
  @Mock private PipelineResult mockResult;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testBigqueryMatcherThatSucceed() throws IOException {
    BigqueryMatcher matcher = spy(new BigqueryMatcher(
        appName, projectId, query, "1f342f9531b4ed978419cf6d3495736a4055b3d9"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockBigqueryClient.jobs()).thenReturn(mockJobs);

    TableRow row = new TableRow();
    row.set("word", "a");
    row.set("count", 2);
    QueryResponse response = new QueryResponse();
    response.setRows(Lists.newArrayList(row));

    when(mockJobs.query(anyString(), any(QueryRequest.class))).thenReturn(mockQuery);
    when(mockQuery.execute()).thenReturn(response);

    assertThat(mockResult, matcher);
    verify(matcher).newBigqueryClient(eq(appName));
    verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
  }

  @Test
  public void testBigqueryMatcherFailsForServiecFails() throws IOException {
    BigqueryMatcher matcher = spy(new BigqueryMatcher(
        appName, projectId, query, "some-checksum"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockBigqueryClient.jobs()).thenReturn(mockJobs);
    when(mockJobs.query(anyString(), any(QueryRequest.class))).thenReturn(mockQuery);
    when(mockQuery.execute()).thenThrow(new IOException());

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to retrieve BigQuery data.");
    assertThat(mockResult, matcher);
    verify(matcher).newBigqueryClient(eq(appName));
    verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
  }

  @Test
  public void testBigqueryMatcherFailsForInvalidQueryResponse() throws IOException {
    BigqueryMatcher matcher = spy(new BigqueryMatcher(
        appName, projectId, query, "some-checksum"));
    doReturn(mockBigqueryClient).when(matcher).newBigqueryClient(anyString());
    when(mockBigqueryClient.jobs()).thenReturn(mockJobs);
    when(mockJobs.query(anyString(), any(QueryRequest.class))).thenReturn(mockQuery);
    when(mockQuery.execute()).thenReturn(null);

    try {
      assertThat(mockResult, matcher);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("was (null)"));
      verify(matcher).newBigqueryClient(eq(appName));
      verify(mockJobs).query(eq(projectId), eq(new QueryRequest().setQuery(query)));
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError is expected.");
  }

  @Test
  public void testNewBigqueryClient() {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    doReturn(new TestCredential())
        .when(matcher).getDefaultCredential(any(HttpTransport.class), any(JsonFactory.class));
    Bigquery client = matcher.newBigqueryClient(appName);
    assertEquals(appName, client.getApplicationName());
  }

  @Test
  public void testHashing() {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));

    TableRow rowOne = new TableRow();
    rowOne.set("word", "a");
    rowOne.set("count", 2);
    TableRow rowTwo = new TableRow();
    rowTwo.set("word", "b");
    rowTwo.set("count", 3);
    List<TableRow> rows = Lists.newArrayList(rowOne, rowTwo);
    assertEquals("56d4167a4f86c2f1b070d4428dce6f8333493856", matcher.hashing(rows));
  }

  @Test
  public void testHashingWithInvalidHashingData() {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));

    List<TableRow> rows = Lists.newArrayList();
    assertTrue(matcher.hashing(rows).isEmpty());
    assertTrue(matcher.hashing(null).isEmpty());
  }

  @Test
  public void testValidateNullArgument() {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected valid argument, but was null");
    matcher.validateArguments(appName, null);
  }

  @Test
  public void testValidateEmptyStringArgument() {
    BigqueryMatcher matcher = spy(
        new BigqueryMatcher(appName, projectId, query, "some-checksum"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected valid argument, but was ");
    matcher.validateArguments(appName, projectId, "");
  }
}
