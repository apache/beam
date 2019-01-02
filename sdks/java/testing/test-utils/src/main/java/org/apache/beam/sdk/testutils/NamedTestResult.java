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
package org.apache.beam.sdk.testutils;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Represents a schema and corresponding test result. Each test may have multiple named results
 * published.
 */
public class NamedTestResult implements TestResult {

  private final String testID;
  private final String timestamp;
  private final String metric;
  private final double value;
  private static final Map<String, String> schema =
      ImmutableMap.<String, String>builder()
          .put("test_id", LegacySQLTypeName.STRING.name())
          .put("timestamp", LegacySQLTypeName.TIMESTAMP.name())
          .put("metric", LegacySQLTypeName.STRING.name())
          .put("value", LegacySQLTypeName.FLOAT.name())
          .build();

  private NamedTestResult(String testID, String timestamp, String metric, double value) {
    this.testID = testID;
    this.timestamp = timestamp;
    this.metric = metric;
    this.value = value;
  }

  /**
   * Creates a NamedTestResult.
   *
   * @param testID Unique identifier for the test run this result belongs to.
   * @param timestamp Time at which this result was sampled. Should be in a BigQuery supported
   *     timestamp format.
   * @param metric Name of this result's value.
   * @param value The actual sampled value.
   */
  public static NamedTestResult create(
      String testID, String timestamp, String metric, double value) {
    return new NamedTestResult(testID, timestamp, metric, value);
  }

  @Override
  public Map<String, Object> toMap() {
    return ImmutableMap.<String, Object>builder()
        .put("test_id", testID)
        .put("timestamp", timestamp)
        .put("metric", metric)
        .put("value", value)
        .build();
  }

  public static Map<String, String> getSchema() {
    return schema;
  }
}
