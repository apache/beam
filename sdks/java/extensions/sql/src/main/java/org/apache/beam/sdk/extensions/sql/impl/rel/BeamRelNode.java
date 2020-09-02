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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

/** A {@link RelNode} that can also give a {@link PTransform} that implements the expression. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public interface BeamRelNode extends RelNode {

  /**
   * Whether the collection of rows represented by this relational expression is bounded (known to
   * be finite) or unbounded (may or may not be finite).
   *
   * @return bounded if and only if all PCollection inputs are bounded
   */
  default PCollection.IsBounded isBounded() {
    return getPCollectionInputs().stream()
            .allMatch(
                rel ->
                    BeamSqlRelUtils.getBeamRelInput(rel).isBounded()
                        == PCollection.IsBounded.BOUNDED)
        ? PCollection.IsBounded.BOUNDED
        : PCollection.IsBounded.UNBOUNDED;
  }

  default List<RelNode> getPCollectionInputs() {
    return getInputs();
  };

  PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform();

  /** Perform a DFS(Depth-First-Search) to find the PipelineOptions config. */
  default Map<String, String> getPipelineOptions() {
    Map<String, String> options = null;
    for (RelNode input : getInputs()) {
      Map<String, String> inputOptions = ((BeamRelNode) input).getPipelineOptions();
      assert inputOptions != null;
      assert options == null || options == inputOptions;
      options = inputOptions;
    }
    return options;
  }

  /**
   * This method is called by {@code
   * org.apache.beam.sdk.extensions.sql.impl.planner.RelMdNodeStats}. This is currently only used in
   * SQLTransform Path (and not JDBC path). When a RelNode wants to calculate its BeamCost or
   * estimate its NodeStats, it may need NodeStat of its inputs. However, it should not call this
   * directly (because maybe its inputs are not physical yet). It should call {@link
   * org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils#getNodeStats(
   * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode,
   * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery)}
   * instead.
   */
  NodeStats estimateNodeStats(RelMetadataQuery mq);

  /**
   * This method is called by {@code
   * org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner.NonCumulativeCostImpl}. This is
   * currently only used in SQLTransform Path (and not JDBC path). This is needed when Calcite Query
   * Planner wants to get the cost of a plan. Instead of calling this directly for a node, if we
   * needed that it should be obtained by calling mq.getNonCumulativeCost. This way RelMetadataQuery
   * will call this method instead of ComputeSelfCost if the handler is set correctly (see {@code
   * org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner#convertToBeamRel(String)})
   */
  BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq);
}
