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
package org.apache.beam.sdk.io.common;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.publishing.BigQueryResultsPublisher;
import org.apache.beam.sdk.testutils.publishing.ConsoleResultPublisher;

/**
 * Contains a flexible mechanism of publishing metrics to BQ and console using suppliers provided in
 * test class using this object.
 */
public class IOITMetrics {

  private final Set<Function<MetricsReader, NamedTestResult>> metricSuppliers;
  private final PipelineResult result;
  private final String namespace;
  private String uuid;
  private String timestamp;

  public IOITMetrics(
      Set<Function<MetricsReader, NamedTestResult>> metricSuppliers,
      PipelineResult result,
      String namespace,
      String uuid,
      String timestamp) {
    this.metricSuppliers = metricSuppliers;
    this.result = result;
    this.namespace = namespace;
    this.uuid = uuid;
    this.timestamp = timestamp;
  }

  public void publish(String bigQueryDataset, String bigQueryTable) {
    MetricsReader reader = new MetricsReader(result, namespace);
    Collection<NamedTestResult> namedTestResults = reader.readAll(metricSuppliers);
    if (bigQueryDataset != null && bigQueryTable != null) {
      BigQueryResultsPublisher.create(bigQueryDataset, NamedTestResult.getSchema())
          .publish(namedTestResults, bigQueryTable);
    }
    ConsoleResultPublisher.publish(namedTestResults, uuid, timestamp);
  }
}
