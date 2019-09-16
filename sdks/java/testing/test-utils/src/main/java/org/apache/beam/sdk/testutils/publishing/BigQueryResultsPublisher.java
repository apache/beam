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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testutils.TestResult;

/** Publishes {@link TestResult}. */
public class BigQueryResultsPublisher {

  private BigQueryClient client;

  private Map<String, String> schema;

  protected BigQueryResultsPublisher(BigQueryClient client, Map<String, String> schema) {
    this.client = client;
    this.schema = schema;
  }

  public static BigQueryResultsPublisher create(String dataset, Map<String, String> schema) {
    return new BigQueryResultsPublisher(BigQueryClient.create(dataset), schema);
  }

  public void publish(TestResult result, String tableName, long nowInMillis) {
    Map<String, Object> row = getRowOfSchema(result);

    // BigQuery requires seconds so we have to divide here
    row.put("timestamp", nowInMillis / 1000);
    client.insertRow(row, schema, tableName);
  }

  public void publish(TestResult result, String tableName) {
    client.insertRow(getRowOfSchema(result), schema, tableName);
  }

  public void publish(Collection<? extends TestResult> results, String tableName) {
    List<Map<String, ?>> records =
        results.stream().map(this::getRowOfSchema).collect(Collectors.toList());
    client.insertAll(records, schema, tableName);
  }

  private Map<String, Object> getRowOfSchema(TestResult result) {
    return result.toMap().entrySet().stream()
        .filter(element -> schema.containsKey(element.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
