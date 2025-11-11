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

import org.apache.beam.sdk.metrics.Metrics.MetricsFlag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class MetricsLineage extends Lineage {

  private final Metric metric;

  public MetricsLineage(final Lineage.LineageDirection direction) {
    // Derive Metrics-specific Type from LineageDirection
    Lineage.Type type =
        (direction == Lineage.LineageDirection.SOURCE) ? Lineage.Type.SOURCE : Lineage.Type.SINK;

    if (MetricsFlag.lineageRollupEnabled()) {
      this.metric =
          Metrics.boundedTrie(
              Lineage.LINEAGE_NAMESPACE,
              direction == Lineage.LineageDirection.SOURCE
                  ? Lineage.Type.SOURCEV2.toString()
                  : Lineage.Type.SINKV2.toString());
    } else {
      this.metric = Metrics.stringSet(Lineage.LINEAGE_NAMESPACE, type.toString());
    }
  }

  @Override
  public void add(final Iterable<String> rollupSegments) {
    ImmutableList<String> segments = ImmutableList.copyOf(rollupSegments);
    if (MetricsFlag.lineageRollupEnabled()) {
      ((BoundedTrie) this.metric).add(segments);
    } else {
      ((StringSet) this.metric).add(String.join("", segments));
    }
  }
}
