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

import java.util.Set;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class BeamSideInputLookupJoinRel extends BeamJoinRel {

  public BeamSideInputLookupJoinRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, left, right, condition, variablesSet, joinType);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    // Should we throw Exception when joinType is LEFT (or) RIGHT (or) FULL?
    return new SideInputLookupJoin();
  }

  private class SideInputLookupJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      Schema schema = CalciteUtils.toSchema(getRowType());

      BeamRelNode seekableRel =
          BeamSqlRelUtils.getBeamRelInput(getInput(seekableInputIndex().get()));
      BeamRelNode nonSeekableRel =
          BeamSqlRelUtils.getBeamRelInput(getInput(nonSeekableInputIndex().get()));

      // Offset field references according to which table is on the left
      int factColOffset =
          nonSeekableInputIndex().get() == 0
              ? 0
              : CalciteUtils.toSchema(seekableRel.getRowType()).getFieldCount();
      int lkpColOffset =
          seekableInputIndex().get() == 0
              ? 0
              : CalciteUtils.toSchema(nonSeekableRel.getRowType()).getFieldCount();

      // HACK: if the input is an immediate instance of a seekable IO, we can do lookups
      // so we ignore the PCollection
      BeamIOSourceRel seekableInput = (BeamIOSourceRel) seekableRel;
      BeamSqlSeekableTable seekableTable = (BeamSqlSeekableTable) seekableInput.getBeamSqlTable();

      // getPCollectionInputs() ensures that there is only one and it is the non-seekable input
      PCollection<Row> nonSeekableInput = pinput.get(0);

      return nonSeekableInput
          .apply(
              "join_as_lookup",
              new BeamJoinTransforms.JoinAsLookup(
                  condition,
                  seekableTable,
                  CalciteUtils.toSchema(seekableInput.getRowType()),
                  schema,
                  factColOffset,
                  lkpColOffset))
          .setRowSchema(schema);
    }
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new BeamSideInputLookupJoinRel(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }
}
