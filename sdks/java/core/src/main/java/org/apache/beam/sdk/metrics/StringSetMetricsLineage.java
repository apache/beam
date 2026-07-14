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
package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.lineage.LineageBase;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Lineage implementation that stores lineage information in {@link StringSet} metrics.
 *
 * <p>Used when {@link Metrics.MetricsFlag#lineageRollupEnabled()} is false.
 */
class StringSetMetricsLineage implements LineageBase {

  private final StringSet metric;

  @SuppressWarnings("unused")
  public StringSetMetricsLineage(PipelineOptions options, Lineage.LineageDirection direction) {
    Lineage.Type type =
        (direction == Lineage.LineageDirection.SOURCE) ? Lineage.Type.SOURCE : Lineage.Type.SINK;
    this.metric = Metrics.stringSet(Lineage.LINEAGE_NAMESPACE, type.toString());
  }

  @Override
  public void add(Iterable<String> rollupSegments) {
    metric.add(String.join("", rollupSegments));
  }
}
