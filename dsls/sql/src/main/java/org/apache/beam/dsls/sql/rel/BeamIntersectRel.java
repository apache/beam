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

package org.apache.beam.dsls.sql.rel;

import java.util.List;

import org.apache.beam.dsls.sql.planner.BeamPipelineCreator;
import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.transform.BeamSQLRow2KvFn;
import org.apache.beam.dsls.sql.transform.SetOperatorFilteringDoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.SetOp;

/**
 * {@code BeamRelNode} to replace a {@code Intersect} node.
 *
 * <p>This is used to combine two SELECT statements, but returns rows only from the
 * first SELECT statement that are identical to a row in the second SELECT statement.
 */
public class BeamIntersectRel extends Intersect implements BeamRelNode {
  public BeamIntersectRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamIntersectRel(getCluster(), traitSet, inputs, all);
  }

  @Override public PCollection<BeamSQLRow> buildBeamPipeline(
      BeamPipelineCreator planCreator) throws Exception {
    List<RelNode> input = getInputs();
    PCollection<BeamSQLRow> leftRows = BeamSQLRelUtils.getBeamRelInput(input.get(0))
        .buildBeamPipeline(planCreator);
    PCollection<BeamSQLRow> rightRows = BeamSQLRelUtils.getBeamRelInput(input.get(1))
        .buildBeamPipeline(planCreator);

    final TupleTag<BeamSQLRow> leftTag = new TupleTag<>();
    final TupleTag<BeamSQLRow> rightTag = new TupleTag<>();
    // co-group
    PCollection<KV<BeamSQLRow, CoGbkResult>> coGbkResultCollection = KeyedPCollectionTuple
        .of(leftTag, leftRows.apply(
            "CreateLeftIndex", MapElements.via(new BeamSQLRow2KvFn(!all))))
        .and(rightTag, rightRows.apply(
            "CreateRightIndex", MapElements.via(new BeamSQLRow2KvFn(!all))))
        .apply(CoGroupByKey.<BeamSQLRow>create());
    PCollection<BeamSQLRow> ret = coGbkResultCollection
        .apply(ParDo.of(new SetOperatorFilteringDoFn(leftTag, rightTag,
            SetOperatorFilteringDoFn.OpType.INTERSECT, all)));
    return ret;
  }
}
