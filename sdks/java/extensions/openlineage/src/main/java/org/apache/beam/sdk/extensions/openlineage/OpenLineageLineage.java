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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LineageBase} plugin that forwards every source/sink FQN reported by Beam IO connectors
 * to OpenLineage, live, from whichever JVM the IO executes in (e.g. a Flink TaskManager). Activated
 * with {@code --lineageType=org.apache.beam.sdk.extensions.openlineage.OpenLineageLineage}.
 *
 * <p>Each FQN is also teed back into the default metrics-based lineage store (both the StringSet
 * and BoundedTrie forms, so {@link Lineage#query} keeps working whether or not the {@code
 * enable_lineage_rollup} experiment is active) and runner-native lineage consumers keep working
 * unchanged.
 *
 * <p>This class is instantiated reflectively by the SDK during worker initialization, so it must
 * never throw: any failure degrades to metrics-only lineage with a warning logged.
 *
 * <p>All JVMs of the same pipeline execution resolve the same run id (see {@link
 * OpenLineageContext}), so events from parallel workers merge into a single run downstream.
 */
public class OpenLineageLineage implements LineageBase {

  private static final Logger LOG = LoggerFactory.getLogger(OpenLineageLineage.class);

  private final PipelineOptions options;
  private final Lineage.LineageDirection direction;
  private final String stringSetMetricName;
  private final String boundedTrieMetricName;
  private volatile boolean emissionBroken;

  public OpenLineageLineage(PipelineOptions options, Lineage.LineageDirection direction) {
    this.options = options;
    this.direction = direction;
    boolean isSource = direction == Lineage.LineageDirection.SOURCE;
    this.stringSetMetricName =
        isSource ? Lineage.Type.SOURCE.toString() : Lineage.Type.SINK.toString();
    this.boundedTrieMetricName =
        isSource ? Lineage.Type.SOURCEV2.toString() : Lineage.Type.SINKV2.toString();
  }

  @Override
  public void add(Iterable<String> rollupSegments) {
    List<String> parts = new ArrayList<>();
    rollupSegments.forEach(parts::add);
    String fqn = String.join("", parts);

    // Tee into the default metrics-based lineage so metrics consumers keep working. Both forms
    // are populated because Lineage.query reads StringSet or BoundedTrie depending on the
    // enable_lineage_rollup experiment.
    Metrics.stringSet(Lineage.LINEAGE_NAMESPACE, stringSetMetricName).add(fqn);
    Metrics.boundedTrie(Lineage.LINEAGE_NAMESPACE, boundedTrieMetricName)
        .add(ImmutableList.copyOf(parts));

    if (emissionBroken) {
      return;
    }
    try {
      // The context is resolved per call (cheap after initialization) rather than captured at
      // construction, because at submission time OpenLineageRunner may replace the context after
      // minting the run id.
      OpenLineageContext context = OpenLineageContext.getOrCreate(options);
      context.onJobSubmitted();
      context.onDatasetDiscovered(
          direction == Lineage.LineageDirection.SOURCE
              ? OpenLineageContext.LineageDirection.INPUT
              : OpenLineageContext.LineageDirection.OUTPUT,
          DataplexFqns.toDatasetIdentifier(fqn));
    } catch (RuntimeException | NoClassDefFoundError e) {
      // Lineage must never fail the pipeline; keep the metrics tee working and stop trying.
      emissionBroken = true;
      LOG.warn("OpenLineage emission disabled after failure; metrics-based lineage continues", e);
    }
  }
}
