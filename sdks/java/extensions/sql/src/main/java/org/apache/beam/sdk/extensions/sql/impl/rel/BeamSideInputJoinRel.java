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

import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class BeamSideInputJoinRel extends BeamJoinRel {

  public BeamSideInputJoinRel(
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
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new BeamSideInputJoinRel(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    // if one of the sides is Bounded & the other is Unbounded
    // then do a sideInput join.
    // When doing a sideInput join, the windowFn does not need to match.
    // Only support INNER JOIN & LEFT OUTER JOIN where left side of the join must be
    // the unbounded & RIGHT OUTER JOIN where right side of the join must be the unbounded
    if (joinType == JoinRelType.FULL) {
      throw new UnsupportedOperationException(
          "FULL OUTER JOIN is not supported when join "
              + "a bounded table with an unbounded table.");
    }

    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);

    if ((joinType == JoinRelType.LEFT && leftRelNode.isBounded() == PCollection.IsBounded.BOUNDED)
        || (joinType == JoinRelType.RIGHT
            && rightRelNode.isBounded() == PCollection.IsBounded.BOUNDED)) {
      throw new UnsupportedOperationException(
          String.format("%s side of an OUTER JOIN must be Unbounded table.", joinType.name()));
    }
    return new SideInputJoin();
  }

  private class SideInputJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      Schema leftSchema = CalciteUtils.toSchema(left.getRowType());
      Schema rightSchema = CalciteUtils.toSchema(right.getRowType());

      PCollectionList<KV<Row, Row>> keyedInputs = pinput.apply(new ExtractJoinKeys());

      PCollection<KV<Row, Row>> extractedLeftRows = keyedInputs.get(0);
      PCollection<KV<Row, Row>> extractedRightRows = keyedInputs.get(1);

      return sideInputJoin(extractedLeftRows, extractedRightRows, leftSchema, rightSchema);
    }
  }

  public PCollection<Row> sideInputJoin(
      PCollection<KV<Row, Row>> extractedLeftRows,
      PCollection<KV<Row, Row>> extractedRightRows,
      Schema leftSchema,
      Schema rightSchema) {
    // we always make the Unbounded table on the left to do the sideInput join
    // (will convert the result accordingly before return)
    boolean swapped = (extractedLeftRows.isBounded() == PCollection.IsBounded.BOUNDED);
    JoinRelType realJoinType =
        (swapped && joinType != JoinRelType.INNER) ? JoinRelType.LEFT : joinType;

    PCollection<KV<Row, Row>> realLeftRows = swapped ? extractedRightRows : extractedLeftRows;
    PCollection<KV<Row, Row>> realRightRows = swapped ? extractedLeftRows : extractedRightRows;

    Row realRightNullRow;
    if (swapped) {
      Schema leftNullSchema = buildNullSchema(leftSchema);

      realRightRows = BeamJoinRel.setValueCoder(realRightRows, SchemaCoder.of(leftNullSchema));
      realRightNullRow = Row.nullRow(leftNullSchema);
    } else {
      Schema rightNullSchema = buildNullSchema(rightSchema);

      realRightRows = BeamJoinRel.setValueCoder(realRightRows, SchemaCoder.of(rightNullSchema));
      realRightNullRow = Row.nullRow(rightNullSchema);
    }

    // swapped still need to pass down because, we need to swap the result back.
    return sideInputJoinHelper(
        realJoinType, realLeftRows, realRightRows, realRightNullRow, swapped);
  }

  private PCollection<Row> sideInputJoinHelper(
      JoinRelType joinType,
      PCollection<KV<Row, Row>> leftRows,
      PCollection<KV<Row, Row>> rightRows,
      Row rightNullRow,
      boolean swapped) {
    final PCollectionView<Map<Row, Iterable<Row>>> rowsView = rightRows.apply(View.asMultimap());

    Schema schema = CalciteUtils.toSchema(getRowType());
    return leftRows
        .apply(
            ParDo.of(
                    new BeamJoinTransforms.SideInputJoinDoFn(
                        joinType, rightNullRow, rowsView, swapped, schema))
                .withSideInputs(rowsView))
        .setRowSchema(schema);
  }
}
