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
import java.util.Map;
import org.apache.beam.sdk.testutils.publishing.InfluxDBPublisher;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a schema and corresponding test result. Each test may have multiple named results
 * published.
 */
public class NamedTestResult implements TestResult {

  private final String testId;

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

  private static final Logger LOG = LoggerFactory.getLogger(NamedTestResult.class);

  private NamedTestResult(String testId, String timestamp, String metric, double value) {
    this.testId = testId;
    this.timestamp = timestamp;
    this.metric = metric;
    this.value = value;
  }

  /**
   * Creates a NamedTestResult.
   *
   * @param testId Unique identifier for the test run this result belongs to.
   * @param timestamp Time at which this result was sampled. Should be in a BigQuery supported
   *     timestamp format.
   * @param metric Name of this result's value.
   * @param value The actual sampled value. Values should be non-negative or -1.0 (imply the value
   *     is not applicable). Other negative values will be reset to -1.0.
   */
  public static NamedTestResult create(
      String testId, String timestamp, String metric, double value) {
    if (value < 0.0 && value != -1.0) {
      LOG.warn("Reset invalid NamedTestResult value {} to -1.0.", value);
      value = -1.0;
    }
    return new NamedTestResult(testId, timestamp, metric, value);
  }

  @Override
  public Map<String, Object> toMap() {
    return ImmutableMap.<String, Object>builder()
        .putAll(tags())
        .putAll(fields())
        .put("timestamp", timestamp)
        .build();
  }

  public static Map<String, String> getSchema() {
    return schema;
  }

  public String getMetric() {
    return metric;
  }

  public double getValue() {
    return value;
  }

  public Map<String, String> tags() {
    return ImmutableMap.of("test_id", testId, "metric", metric);
  }

  public Map<String, Number> fields() {
    return ImmutableMap.of("value", value);
  }

  /** Convert this result to InfluxDB data point. */
  public InfluxDBPublisher.DataPoint toInfluxDBDataPoint(String measurement) {
    return InfluxDBPublisher.dataPoint(measurement, tags(), fields(), null);
  }
}
