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
package org.apache.beam.sdk.testutils.publishing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testutils.TestResult;
import org.apache.beam.sdk.testutils.fakes.FakeBigQueryClient;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryResultsPublisher}. */
@RunWith(JUnit4.class)
public class BigQueryResultsPublisherTest {

  private static final String TABLE_NAME = "table";

  private BigQueryResultsPublisher publisher;

  private FakeBigQueryClient bigQueryClient;

  @Before
  public void setUp() {
    ImmutableMap<String, String> schema =
        ImmutableMap.<String, String>builder()
            .put("timestamp", "timestamp")
            .put("field1", "string")
            .build();

    this.bigQueryClient = new FakeBigQueryClient();
    this.publisher = new BigQueryResultsPublisher(bigQueryClient, schema);
  }

  @Test
  public void testPublishRowWithTimestampField() {
    long now = 1000L;

    publisher.publish(new SampleTestResult("a", "b"), TABLE_NAME, now);

    Map<String, ?> rowInTable = bigQueryClient.getRows(TABLE_NAME).get(0);
    assertEquals(2, rowInTable.entrySet().size());
    assertEquals(1L, rowInTable.get("timestamp"));
    assertEquals("a", rowInTable.get("field1"));
  }

  @Test
  public void testPublishRowWithoutTimestamp() {
    publisher.publish(new SampleTestResult("a", "b"), TABLE_NAME);

    Map<String, ?> rowInTable = bigQueryClient.getRows(TABLE_NAME).get(0);
    assertEquals(1, rowInTable.entrySet().size());
    assertEquals("a", rowInTable.get("field1"));
  }

  @Test
  public void testRowDoesntContainFieldsNotSpecifiedInSchema() {
    publisher.publish(new SampleTestResult("a", "b"), TABLE_NAME);

    Map<String, ?> rowInTable = bigQueryClient.getRows(TABLE_NAME).get(0);
    assertNull(rowInTable.get("field2"));
  }

  @Test
  public void testPublishCollectionOfRecords() {
    List<SampleTestResult> results =
        Arrays.asList(new SampleTestResult("a", "b"), new SampleTestResult("a", "b"));

    publisher.publish(results, TABLE_NAME);

    assertEquals(2, bigQueryClient.getRows(TABLE_NAME).size());
  }

  private static class SampleTestResult implements TestResult {

    private String field1;

    private String field2;

    SampleTestResult(String field1, String field2) {
      this.field1 = field1;
      this.field2 = field2;
    }

    @Override
    public Map<String, Object> toMap() {
      return ImmutableMap.<String, Object>builder()
          .put("field1", field1)
          .put("field2", field2)
          .build();
    }
  }
}
