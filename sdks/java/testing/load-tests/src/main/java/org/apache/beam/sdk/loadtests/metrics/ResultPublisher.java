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
package org.apache.beam.sdk.loadtests.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.publishing.BigQueryClient;

/** Provides ways to publish metrics gathered during test invocation. */
public class ResultPublisher {

  private static final ImmutableMap<String, String> BIG_QUERY_SCHEMA =
      ImmutableMap.<String, String>builder()
          .put("timestamp", "timestamp")
          .put("runtime", "float")
          .put("total_bytes_count", "integer")
          .build();

  private static final String RUNTIME = "runtime";
  private static final String TOTAL_BYTES_COUNT = "totalBytes.count";

  private MetricsReader reader;

  public ResultPublisher(PipelineResult result, String namespace) {
    this.reader = new MetricsReader(result, namespace);
  }

  public void toConsole() {
    long totalBytes = reader.getCounterMetric(TOTAL_BYTES_COUNT);
    long startTime = reader.getStartTimeMetric(RUNTIME);
    long endTime = reader.getEndTimeMetric(RUNTIME);

    System.out.println(String.format("Total bytes: %s", totalBytes));
    System.out.println(String.format("Total time (millis): %s", endTime - startTime));
  }

  public void toBigQuery(String dataset, String table) {
    BigQueryClient client = BigQueryClient.create(dataset);

    long now = System.currentTimeMillis();
    long totalBytes = reader.getCounterMetric(TOTAL_BYTES_COUNT);
    long startTime = reader.getStartTimeMetric(RUNTIME);
    long endTime = reader.getEndTimeMetric(RUNTIME);

    ImmutableMap<String, Object> row =
        ImmutableMap.<String, Object>builder()
            .put("timestamp", now / 1000)
            .put("runtime", endTime - startTime)
            .put("total_bytes_count", totalBytes)
            .build();

    client.insertRow(row, BIG_QUERY_SCHEMA, table);
  }
}
