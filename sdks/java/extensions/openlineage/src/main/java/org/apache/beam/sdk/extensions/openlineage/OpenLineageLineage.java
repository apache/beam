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
package org.apache.beam.sdk.extensions.openlineage;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.lineage.LineageBase;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A {@link LineageBase} plugin that forwards every source/sink FQN reported by Beam IO connectors
 * to OpenLineage, live, from whichever JVM the IO executes in (e.g. a Flink TaskManager). Activated
 * with {@code --lineageType=org.apache.beam.sdk.extensions.openlineage.OpenLineageLineage}.
 *
 * <p>Each FQN is also teed back into the default metrics-based lineage store, so {@link
 * Lineage#query} and runner-native lineage consumers keep working unchanged.
 *
 * <p>All JVMs of the same pipeline execution resolve the same run id (see {@link
 * OpenLineageContext}), so events from parallel workers merge into a single run downstream.
 */
public class OpenLineageLineage implements LineageBase {

  private final Lineage.LineageDirection direction;
  private final String metricName;
  private final OpenLineageContext context;

  public OpenLineageLineage(PipelineOptions options, Lineage.LineageDirection direction) {
    this.direction = direction;
    this.metricName =
        direction == Lineage.LineageDirection.SOURCE
            ? Lineage.Type.SOURCE.toString()
            : Lineage.Type.SINK.toString();
    this.context = OpenLineageContext.getOrCreate(options);
    this.context.onJobSubmitted();
  }

  @Override
  public void add(Iterable<String> rollupSegments) {
    List<String> parts = new ArrayList<>();
    rollupSegments.forEach(parts::add);
    String fqn = String.join("", parts);

    // Tee into the default metrics-based lineage so metrics consumers keep working.
    Metrics.stringSet(Lineage.LINEAGE_NAMESPACE, metricName).add(fqn);

    context.onDatasetDiscovered(
        direction == Lineage.LineageDirection.SOURCE
            ? OpenLineageContext.LineageDirection.INPUT
            : OpenLineageContext.LineageDirection.OUTPUT,
        DataplexFqns.toDatasetIdentifier(fqn));
  }
}
