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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import java.math.BigInteger;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests for {@link BigqueryMatcher}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BigqueryClient.class)
public class BigqueryMatcherTest {
  private final String appName = "test-app";
  private final String projectId = "test-project";
  private final String query = "test-query";

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private BigqueryClient mockBigqueryClient;
  @Mock private PipelineResult mockResult;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    PowerMockito.mockStatic(BigqueryClient.class);
    when(BigqueryClient.getClient(anyString())).thenReturn(mockBigqueryClient);
  }

  @Test
  public void testBigqueryMatcherThatSucceeds() throws Exception {
    BigqueryMatcher matcher =
        spy(
            new BigqueryMatcher(
                appName, projectId, query, "9bb47f5c90d2a99cad526453dff5ed5ec74650dc"));
    when(mockBigqueryClient.queryWithRetries(anyString(), anyString()))
        .thenReturn(createResponseContainingTestData());

    assertThat(mockResult, matcher);
  }

  @Test
  public void testBigqueryMatcherFailsForChecksumMismatch() throws Exception {
    BigqueryMatcher matcher =
        spy(new BigqueryMatcher(appName, projectId, query, "incorrect-checksum"));

    when(mockBigqueryClient.queryWithRetries(anyString(), anyString()))
        .thenReturn(createResponseContainingTestData());

    thrown.expect(AssertionError.class);
    thrown.expectMessage("Total number of rows are: 1");
    thrown.expectMessage("abc");

    assertThat(mockResult, matcher);
  }

  @Test
  public void testBigqueryMatcherFailsWhenQueryJobNotComplete() throws Exception {
    BigqueryMatcher matcher = spy(new BigqueryMatcher(appName, projectId, query, "some-checksum"));
    when(mockBigqueryClient.queryWithRetries(anyString(), anyString()))
        .thenReturn(new QueryResponse().setJobComplete(false));

    thrown.expect(AssertionError.class);
    thrown.expectMessage("The query job hasn't completed.");
    thrown.expectMessage("jobComplete=false");

    assertThat(mockResult, matcher);
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
