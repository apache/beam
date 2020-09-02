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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.Table;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * This is the implementation of NodeStatsMetadata. Methods to estimate rate and row count for
 * Calcite's logical nodes be implemented here.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class RelMdNodeStats implements MetadataHandler<NodeStatsMetadata> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          NodeStatsMetadata.METHOD, new RelMdNodeStats());

  @Override
  public MetadataDef<NodeStatsMetadata> getDef() {
    return NodeStatsMetadata.DEF;
  }

  @SuppressWarnings("UnusedDeclaration")
  public NodeStats getNodeStats(RelNode rel, RelMetadataQuery mq) {

    if (rel instanceof BeamRelNode) {
      return this.getBeamNodeStats((BeamRelNode) rel, mq);
    }

    // We can later define custom methods for all different RelNodes to prevent hitting this point.
    // Similar to RelMdRowCount in calcite.

    return NodeStats.UNKNOWN;
  }

  private NodeStats getBeamNodeStats(BeamRelNode rel, RelMetadataQuery mq) {

    // Removing the unknown results.
    // Calcite caches previous results in mq.map. This is done to prevent cyclic calls of this
    // method and also improving the performance. However, we might have returned an unknown result
    // because one of the inputs of the node was unknown (it is a logical node that we have not
    // implemented getNodeStats for it). Later we should not get the Unknown, therefore we need to
    // remove unknown results everyTime that this method is called.
    // Results are also cached in CachingRelMetadataProvider because calcite PlannerImpl#Transform
    // wraps the metadata provider with CachingRelMetadataProvider. However,
    // CachingRelMetadataProvider checks timestamp before returning previous results. Therefore,
    // there wouldn't be a problem in that case.
    Set<Table.Cell<RelNode, List, Object>> cells = mq.map.cellSet();
    List<Table.Cell<RelNode, List, Object>> keys = new ArrayList<>(cells.size());
    for (Table.Cell<RelNode, List, Object> cell : cells) {
      if (cell == null) {
        continue;
      }
      Object rawValue = cell.getValue();
      if (!(rawValue instanceof NodeStats)) {
        continue;
      }
      NodeStats nodeStats = (NodeStats) rawValue;
      if (nodeStats.isUnknown()) {
        keys.add(cell);
      }
    }
    //    List<Table.Cell<RelNode, List, Object>> keys =
    //        mq.map.cellSet().stream()
    //            .filter(entry -> entry.getValue() instanceof NodeStats)
    //            .filter(entry -> ((NodeStats) entry.getValue()).isUnknown())
    //            .collect(Collectors.toList());

    // === > Task :sdks:java:extensions:sql:compileJava
    // ===   error: [dereference.of.nullable] dereference of possibly-null reference
    //       ((NodeStats)entry.getValue())
    // ===   .filter(entry -> ((NodeStats) entry.getValue()).isUnknown())
    // ===   ^

    keys.forEach(cell -> mq.map.remove(cell.getRowKey(), cell.getColumnKey()));

    return rel.estimateNodeStats(mq);
  }
}
