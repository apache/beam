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

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOSinkRule;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.prepare.Prepare;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.TableModify;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql2rel.RelStructuredTypeFlattener;

/** BeamRelNode to replace a {@code TableModify} node. */
public class BeamIOSinkRel extends TableModify
    implements BeamRelNode, RelStructuredTypeFlattener.SelfFlatteningRel {

  private final BeamSqlTable sqlTable;
  private final Map<String, String> pipelineOptions;
  private boolean isFlattening = false;

  public BeamIOSinkRel(
      RelOptCluster cluster,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode child,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened,
      BeamSqlTable sqlTable,
      Map<String, String> pipelineOptions) {
    super(
        cluster,
        cluster.traitSetOf(BeamLogicalConvention.INSTANCE),
        table,
        catalogReader,
        child,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened);
    this.sqlTable = sqlTable;
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    return BeamSqlRelUtils.getNodeStats(this.input, mq);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats inputEstimates = BeamSqlRelUtils.getNodeStats(this.input, mq);
    return BeamCostModel.FACTORY.makeCost(inputEstimates.getRowCount(), inputEstimates.getRate());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    boolean flattened = isFlattened() || isFlattening;
    BeamIOSinkRel newRel =
        new BeamIOSinkRel(
            getCluster(),
            getTable(),
            getCatalogReader(),
            sole(inputs),
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList(),
            flattened,
            sqlTable,
            pipelineOptions);
    newRel.traitSet = traitSet;
    return newRel;
  }

  @Override
  public void flattenRel(RelStructuredTypeFlattener flattener) {
    // rewriteGeneric calls this.copy. Setting isFlattining passes
    // this context into copy for modification of the flattened flag.
    isFlattening = true;
    flattener.rewriteGeneric(this);
    isFlattening = false;
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(BeamIOSinkRule.INSTANCE);
    super.register(planner);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamIOSinkRel.class.getSimpleName(),
          pinput);
      PCollection<Row> input = pinput.get(0);

      sqlTable.buildIOWriter(input);

      return input;
    }
  }

  @Override
  public Map<String, String> getPipelineOptions() {
    return pipelineOptions;
  }
}
