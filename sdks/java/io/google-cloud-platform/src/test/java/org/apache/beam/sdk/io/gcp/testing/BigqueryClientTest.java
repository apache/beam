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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import java.io.IOException;
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
}
