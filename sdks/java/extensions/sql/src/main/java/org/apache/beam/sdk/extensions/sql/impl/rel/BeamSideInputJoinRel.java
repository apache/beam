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
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join.FieldsEqual;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.CorrelationId;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * A {@code BeamJoinRel} which does sideinput Join
 *
 * <p>This Join Covers the case:
 *
 * <ul>
 *   <li>BoundedTable JOIN UnboundedTable
 * </ul>
 *
 * <p>{@code sideInput} is utilized to implement the join, so there are some constraints:
 *
 * <ul>
 *   <li>{@code FULL OUTER JOIN} is not supported.
 *   <li>If it's a {@code LEFT OUTER JOIN}, the unbounded table should on the left side.
 *   <li>If it's a {@code RIGHT OUTER JOIN}, the unbounded table should on the right side.
 * </ul>
 *
 * <p>General constraints:
 *
 * <ul>
 *   <li>Only equi-join is supported.
 *   <li>CROSS JOIN is not supported.
 * </ul>
 */
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
    if (leftRelNode.isBounded() == IsBounded.UNBOUNDED
        && rightRelNode.isBounded() == IsBounded.UNBOUNDED) {
      throw new UnsupportedOperationException(
          "Side input join can only be used if one table is bounded.");
    }
    return new SideInputJoin();
  }

  private class SideInputJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      Schema leftSchema = pinput.get(0).getSchema();
      Schema rightSchema = pinput.get(1).getSchema();
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

      // extract the join fields
      List<Pair<RexNode, RexNode>> pairs = extractJoinRexNodes(condition);
      int leftRowColumnCount = BeamSqlRelUtils.getBeamRelInput(left).getRowType().getFieldCount();
      FieldAccessDescriptor leftKeyFields =
          BeamJoinTransforms.getJoinColumns(true, pairs, 0, leftSchema);
      FieldAccessDescriptor rightKeyFields =
          BeamJoinTransforms.getJoinColumns(false, pairs, leftRowColumnCount, rightSchema);

      return sideInputJoin(leftRows, rightRows, leftKeyFields, rightKeyFields);
    }
  }

  public PCollection<Row> sideInputJoin(
      PCollection<Row> leftRows,
      PCollection<Row> rightRows,
      FieldAccessDescriptor leftKeyFields,
      FieldAccessDescriptor rightKeyFields) {
    // we always make the Unbounded table on the left to do the sideInput join
    // (will convert the result accordingly before return)
    boolean swapped = (leftRows.isBounded() == PCollection.IsBounded.BOUNDED);
    JoinRelType realJoinType = joinType;
    if (swapped && joinType != JoinRelType.INNER) {
      Preconditions.checkArgument(realJoinType != JoinRelType.LEFT);
      realJoinType = JoinRelType.LEFT;
    }

    PCollection<Row> realLeftRows = swapped ? rightRows : leftRows;
    PCollection<Row> realRightRows = swapped ? leftRows : rightRows;
    FieldAccessDescriptor realLeftKeyFields = swapped ? rightKeyFields : leftKeyFields;
    FieldAccessDescriptor realRightKeyFields = swapped ? leftKeyFields : rightKeyFields;

    PCollection<Row> joined;
    switch (realJoinType) {
      case INNER:
        joined =
            realLeftRows.apply(
                org.apache.beam.sdk.schemas.transforms.Join.<Row, Row>innerBroadcastJoin(
                        realRightRows)
                    .on(FieldsEqual.left(realLeftKeyFields).right(realRightKeyFields)));
        break;
      case LEFT:
        joined =
            realLeftRows.apply(
                org.apache.beam.sdk.schemas.transforms.Join.<Row, Row>leftOuterBroadcastJoin(
                        realRightRows)
                    .on(FieldsEqual.left(realLeftKeyFields).right(realRightKeyFields)));
        break;
      default:
        throw new RuntimeException("Unexpected join type " + realJoinType);
    }
    Schema schema = CalciteUtils.toSchema(getRowType());

    String lhsSelect = org.apache.beam.sdk.schemas.transforms.Join.LHS_TAG + ".*";
    String rhsSelect = org.apache.beam.sdk.schemas.transforms.Join.RHS_TAG + ".*";
    PCollection<Row> selected =
        (!swapped)
            ? joined.apply(Select.<Row>fieldNames(lhsSelect, rhsSelect).withOutputSchema(schema))
            : joined.apply(Select.<Row>fieldNames(rhsSelect, lhsSelect).withOutputSchema(schema));
    return selected;
  }
}
