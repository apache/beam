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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.CorrelationId;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;

/**
 * A {@code BeamJoinRel} that implements a BOUNDED cartesian (CROSS) join, used for the literal-TRUE
 * join conditions that Calcite emits when it decorrelates uncorrelated scalar / IN / EXISTS
 * subqueries and lateral joins.
 *
 * <p>Only supported when BOTH inputs are bounded. The right side is broadcast as a side input and
 * the left side is iterated; every (left, right) pair is emitted as one combined row (left values
 * followed by right values).
 *
 * <p>First increment: INNER join with an always-true condition. A non-trivial (residual) condition
 * is not yet evaluated here.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamCrossJoinRel extends BeamJoinRel {

  public BeamCrossJoinRel(
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
    return new BeamCrossJoinRel(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    if (leftRelNode.isBounded() != PCollection.IsBounded.BOUNDED
        || rightRelNode.isBounded() != PCollection.IsBounded.BOUNDED) {
      throw new UnsupportedOperationException(
          "Bounded cross join requires both inputs to be bounded.");
    }
    if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
      throw new UnsupportedOperationException(
          "Only INNER and LEFT cross join are supported for now, got " + joinType.name());
    }
    if (!condition.isAlwaysTrue()) {
      throw new UnsupportedOperationException(
          "Cross join with a residual (non-equi) condition is not supported yet.");
    }
    return new CrossJoin();
  }

  private class CrossJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      PCollection<Row> leftRows =
          pinput
              .get(0)
              .apply(
                  "left_TimestampCombiner",
                  Window.<Row>configure().withTimestampCombiner(TimestampCombiner.EARLIEST));
      PCollection<Row> rightRows =
          pinput
              .get(1)
              .apply(
                  "right_TimestampCombiner",
                  Window.<Row>configure().withTimestampCombiner(TimestampCombiner.EARLIEST));

      final Schema outputSchema = CalciteUtils.toSchema(getRowType());
      final PCollectionView<List<Row>> rightView = rightRows.apply(View.asList());
      // For a LEFT cross join, a left row with no matching right rows is emitted once with the
      // right columns NULL-padded (the uncorrelated-scalar-subquery shape Calcite decorrelates to).
      final boolean leftJoin = joinType == JoinRelType.LEFT;
      final int rightFieldCount = getRowType().getFieldCount() - left.getRowType().getFieldCount();

      return leftRows
          .apply(
              ParDo.of(new CrossJoinDoFn(outputSchema, rightView, leftJoin, rightFieldCount))
                  .withSideInputs(rightView))
          .setRowSchema(outputSchema);
    }
  }

  /**
   * Emits the cartesian product of each left row with the broadcast right rows. A static {@link
   * DoFn} (not an anonymous/inner class) so it does not capture the non-serializable enclosing rel.
   */
  private static class CrossJoinDoFn extends DoFn<Row, Row> {
    private final Schema outputSchema;
    private final PCollectionView<List<Row>> rightView;
    private final boolean leftJoin;
    private final int rightFieldCount;

    CrossJoinDoFn(
        Schema outputSchema,
        PCollectionView<List<Row>> rightView,
        boolean leftJoin,
        int rightFieldCount) {
      this.outputSchema = outputSchema;
      this.rightView = rightView;
      this.leftJoin = leftJoin;
      this.rightFieldCount = rightFieldCount;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row leftRow = c.element();
      List<Row> rightRowList = c.sideInput(rightView);
      for (Row rightRow : rightRowList) {
        c.output(
            Row.withSchema(outputSchema)
                .addValues(leftRow.getBaseValues())
                .addValues(rightRow.getBaseValues())
                .build());
      }
      if (leftJoin && rightRowList.isEmpty()) {
        c.output(
            Row.withSchema(outputSchema)
                .addValues(leftRow.getBaseValues())
                .addValues(Collections.nCopies(rightFieldCount, null))
                .build());
      }
    }
  }
}
