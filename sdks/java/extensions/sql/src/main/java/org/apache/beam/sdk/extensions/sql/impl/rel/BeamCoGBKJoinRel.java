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

import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.joda.time.Duration.ZERO;

import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join.FieldsEqual;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.CorrelationId;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Pair;

/**
 * A {@code BeamJoinRel} which does CoGBK Join
 *
 * <p>This Join Covers the cases:
 *
 * <ul>
 *   <li>BoundedTable JOIN BoundedTable
 *   <li>UnboundedTable JOIN UnboundedTable
 * </ul>
 *
 * <p>A CoGBK join is utilized as long as the windowFn of the both sides match. For more info refer
 * <a href="https://issues.apache.org/jira/browse/BEAM-3345">BEAM-3345</a>
 *
 * <p>General constraints:
 *
 * <ul>
 *   <li>Only equi-join is supported.
 *   <li>CROSS JOIN is not supported.
 * </ul>
 */
public class BeamCoGBKJoinRel extends BeamJoinRel {

  public BeamCoGBKJoinRel(
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
    return new StandardJoin();
  }

  private class StandardJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {

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

      WindowFn leftWinFn = leftRows.getWindowingStrategy().getWindowFn();
      WindowFn rightWinFn = rightRows.getWindowingStrategy().getWindowFn();

      try {
        leftWinFn.verifyCompatibility(rightWinFn);
      } catch (IncompatibleWindowException e) {
        throw new IllegalArgumentException(
            "WindowFns must match for a bounded-vs-bounded/unbounded-vs-unbounded join.", e);
      }

      verifySupportedTrigger(leftRows);
      verifySupportedTrigger(rightRows);

      return standardJoin(leftRows, rightRows, leftKeyFields, rightKeyFields);
    }
  }

  private <T> void verifySupportedTrigger(PCollection<T> pCollection) {
    WindowingStrategy windowingStrategy = pCollection.getWindowingStrategy();

    if (UNBOUNDED.equals(pCollection.isBounded()) && !triggersOncePerWindow(windowingStrategy)) {
      throw new UnsupportedOperationException(
          "Joining unbounded PCollections is currently only supported for "
              + "non-global windows with triggers that are known to produce output once per window,"
              + "such as the default trigger with zero allowed lateness. "
              + "In these cases Beam can guarantee it joins all input elements once per window. "
              + windowingStrategy
              + " is not supported");
    }
  }

  private boolean triggersOncePerWindow(WindowingStrategy windowingStrategy) {
    Trigger trigger = windowingStrategy.getTrigger();

    return !(windowingStrategy.getWindowFn() instanceof GlobalWindows)
        && trigger instanceof DefaultTrigger
        && ZERO.equals(windowingStrategy.getAllowedLateness());
  }

  private PCollection<Row> standardJoin(
      PCollection<Row> leftRows,
      PCollection<Row> rightRows,
      FieldAccessDescriptor leftKeys,
      FieldAccessDescriptor rightKeys) {
    PCollection<Row> joinedRows = null;

    switch (joinType) {
      case LEFT:
        joinedRows =
            leftRows.apply(
                org.apache.beam.sdk.schemas.transforms.Join.<Row, Row>leftOuterJoin(rightRows)
                    .on(FieldsEqual.left(leftKeys).right(rightKeys)));
        break;
      case RIGHT:
        joinedRows =
            leftRows.apply(
                org.apache.beam.sdk.schemas.transforms.Join.<Row, Row>rightOuterJoin(rightRows)
                    .on(FieldsEqual.left(leftKeys).right(rightKeys)));
        break;
      case FULL:
        joinedRows =
            leftRows.apply(
                org.apache.beam.sdk.schemas.transforms.Join.<Row, Row>fullOuterJoin(rightRows)
                    .on(FieldsEqual.left(leftKeys).right(rightKeys)));
        break;
      case INNER:
      default:
        joinedRows =
            leftRows.apply(
                org.apache.beam.sdk.schemas.transforms.Join.<Row, Row>innerJoin(rightRows)
                    .on(FieldsEqual.left(leftKeys).right(rightKeys)));
    }

    // Flatten the lhs and rhs fields into a single row.
    return joinedRows.apply(
        Select.<Row>fieldNames(
                org.apache.beam.sdk.schemas.transforms.Join.LHS_TAG + ".*",
                org.apache.beam.sdk.schemas.transforms.Join.RHS_TAG + ".*")
            .withOutputSchema(CalciteUtils.toSchema(getRowType())));
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new BeamCoGBKJoinRel(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }
}
