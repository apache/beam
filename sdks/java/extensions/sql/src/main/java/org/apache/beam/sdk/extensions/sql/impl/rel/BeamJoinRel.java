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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.joda.time.Duration.ZERO;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * {@code BeamRelNode} to replace a {@code Join} node.
 *
 * <p>Support for join can be categorized into 3 cases:
 *
 * <ul>
 *   <li>BoundedTable JOIN BoundedTable
 *   <li>UnboundedTable JOIN UnboundedTable
 *   <li>BoundedTable JOIN UnboundedTable
 * </ul>
 *
 * <p>For the first two cases, a standard join is utilized as long as the windowFn of the both sides
 * match.
 *
 * <p>For the third case, {@code sideInput} is utilized to implement the join, so there are some
 * constraints:
 *
 * <ul>
 *   <li>{@code FULL OUTER JOIN} is not supported.
 *   <li>If it's a {@code LEFT OUTER JOIN}, the unbounded table should on the left side.
 *   <li>If it's a {@code RIGHT OUTER JOIN}, the unbounded table should on the right side.
 * </ul>
 *
 * <p>There are also some general constraints:
 *
 * <ul>
 *   <li>Only equi-join is supported.
 *   <li>CROSS JOIN is not supported.
 * </ul>
 */
public class BeamJoinRel extends Join implements BeamRelNode {

  public BeamJoinRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new BeamJoinRel(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }

  @Override
  public List<RelNode> getPCollectionInputs() {
    if (isSideInputJoin()) {
      return ImmutableList.of(BeamSqlRelUtils.getBeamRelInput(left));
    }
    return BeamRelNode.super.getPCollectionInputs();
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private boolean isSideInputJoin() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    return !seekable(leftRelNode) && seekable(rightRelNode);
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
      final BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);

      if (isSideInputJoin()) {
        checkArgument(pinput.size() == 1, "More than one input received for side input join");
        return joinAsLookup(leftRelNode, rightRelNode, pinput.get(0))
            .setCoder(CalciteUtils.toBeamSchema(getRowType()).getRowCoder());
      }

      Schema leftSchema = CalciteUtils.toBeamSchema(left.getRowType());
      assert pinput.size() == 2;
      PCollection<Row> leftRows = pinput.get(0);
      PCollection<Row> rightRows = pinput.get(1);

      verifySupportedTrigger(leftRows);
      verifySupportedTrigger(rightRows);

      WindowFn leftWinFn = leftRows.getWindowingStrategy().getWindowFn();
      WindowFn rightWinFn = rightRows.getWindowingStrategy().getWindowFn();

      // extract the join fields
      List<Pair<Integer, Integer>> pairs =
          extractJoinColumns(leftRelNode.getRowType().getFieldCount());

      // build the extract key type
      // the name of the join field is not important
      Schema extractKeySchema =
          pairs.stream().map(pair -> leftSchema.getField(pair.getKey())).collect(toSchema());

      Coder extractKeyRowCoder = extractKeySchema.getRowCoder();

      // BeamSqlRow -> KV<BeamSqlRow, BeamSqlRow>
      PCollection<KV<Row, Row>> extractedLeftRows =
          leftRows
              .apply(
                  "left_ExtractJoinFields",
                  MapElements.via(new BeamJoinTransforms.ExtractJoinFields(true, pairs)))
              .setCoder(KvCoder.of(extractKeyRowCoder, leftRows.getCoder()));

      PCollection<KV<Row, Row>> extractedRightRows =
          rightRows
              .apply(
                  "right_ExtractJoinFields",
                  MapElements.via(new BeamJoinTransforms.ExtractJoinFields(false, pairs)))
              .setCoder(KvCoder.of(extractKeyRowCoder, rightRows.getCoder()));

      // prepare the NullRows
      Row leftNullRow = buildNullRow(leftRelNode);
      Row rightNullRow = buildNullRow(rightRelNode);

      // a regular join
      if ((leftRows.isBounded() == PCollection.IsBounded.BOUNDED
              && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)
          || (leftRows.isBounded() == UNBOUNDED && rightRows.isBounded() == UNBOUNDED)) {
        try {
          leftWinFn.verifyCompatibility(rightWinFn);
        } catch (IncompatibleWindowException e) {
          throw new IllegalArgumentException(
              "WindowFns must match for a bounded-vs-bounded/unbounded-vs-unbounded join.", e);
        }

        return standardJoin(extractedLeftRows, extractedRightRows, leftNullRow, rightNullRow);
      } else if ((leftRows.isBounded() == PCollection.IsBounded.BOUNDED
              && rightRows.isBounded() == UNBOUNDED)
          || (leftRows.isBounded() == UNBOUNDED
              && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)) {
        // if one of the sides is Bounded & the other is Unbounded
        // then do a sideInput join
        // when doing a sideInput join, the windowFn does not need to match
        // Only support INNER JOIN & LEFT OUTER JOIN where left side of the join must be
        // the unbounded
        if (joinType == JoinRelType.FULL) {
          throw new UnsupportedOperationException(
              "FULL OUTER JOIN is not supported when join "
                  + "a bounded table with an unbounded table.");
        }

        if ((joinType == JoinRelType.LEFT && leftRows.isBounded() == PCollection.IsBounded.BOUNDED)
            || (joinType == JoinRelType.RIGHT
                && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)) {
          throw new UnsupportedOperationException(
              "LEFT side of an OUTER JOIN must be Unbounded table.");
        }

        return sideInputJoin(extractedLeftRows, extractedRightRows, leftNullRow, rightNullRow);
      } else {
        throw new UnsupportedOperationException(
            "The inputs to the JOIN have un-joinnable windowFns: " + leftWinFn + ", " + rightWinFn);
      }
    }
  }

  private void verifySupportedTrigger(PCollection<Row> pCollection) {
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
      PCollection<KV<Row, Row>> extractedLeftRows,
      PCollection<KV<Row, Row>> extractedRightRows,
      Row leftNullRow,
      Row rightNullRow) {
    PCollection<KV<Row, KV<Row, Row>>> joinedRows = null;
    switch (joinType) {
      case LEFT:
        joinedRows =
            org.apache.beam.sdk.extensions.joinlibrary.Join.leftOuterJoin(
                extractedLeftRows, extractedRightRows, rightNullRow);
        break;
      case RIGHT:
        joinedRows =
            org.apache.beam.sdk.extensions.joinlibrary.Join.rightOuterJoin(
                extractedLeftRows, extractedRightRows, leftNullRow);
        break;
      case FULL:
        joinedRows =
            org.apache.beam.sdk.extensions.joinlibrary.Join.fullOuterJoin(
                extractedLeftRows, extractedRightRows, leftNullRow, rightNullRow);
        break;
      case INNER:
      default:
        joinedRows =
            org.apache.beam.sdk.extensions.joinlibrary.Join.innerJoin(
                extractedLeftRows, extractedRightRows);
        break;
    }

    PCollection<Row> ret =
        joinedRows
            .apply(
                "JoinParts2WholeRow", MapElements.via(new BeamJoinTransforms.JoinParts2WholeRow()))
            .setCoder(CalciteUtils.toBeamSchema(getRowType()).getRowCoder());
    return ret;
  }

  public PCollection<Row> sideInputJoin(
      PCollection<KV<Row, Row>> extractedLeftRows,
      PCollection<KV<Row, Row>> extractedRightRows,
      Row leftNullRow,
      Row rightNullRow) {
    // we always make the Unbounded table on the left to do the sideInput join
    // (will convert the result accordingly before return)
    boolean swapped = (extractedLeftRows.isBounded() == PCollection.IsBounded.BOUNDED);
    JoinRelType realJoinType =
        (swapped && joinType != JoinRelType.INNER) ? JoinRelType.LEFT : joinType;

    PCollection<KV<Row, Row>> realLeftRows = swapped ? extractedRightRows : extractedLeftRows;
    PCollection<KV<Row, Row>> realRightRows = swapped ? extractedLeftRows : extractedRightRows;
    Row realRightNullRow = swapped ? leftNullRow : rightNullRow;

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

    PCollection<Row> ret =
        leftRows
            .apply(
                ParDo.of(
                        new BeamJoinTransforms.SideInputJoinDoFn(
                            joinType, rightNullRow, rowsView, swapped))
                    .withSideInputs(rowsView))
            .setCoder(CalciteUtils.toBeamSchema(getRowType()).getRowCoder());

    return ret;
  }

  private Row buildNullRow(BeamRelNode relNode) {
    Schema leftType = CalciteUtils.toBeamSchema(relNode.getRowType());
    return Row.nullRow(leftType);
  }

  private List<Pair<Integer, Integer>> extractJoinColumns(int leftRowColumnCount) {
    // it's a CROSS JOIN because: condition == true
    if (condition instanceof RexLiteral && (Boolean) ((RexLiteral) condition).getValue()) {
      throw new UnsupportedOperationException("CROSS JOIN is not supported!");
    }

    RexCall call = (RexCall) condition;
    List<Pair<Integer, Integer>> pairs = new ArrayList<>();
    if ("AND".equals(call.getOperator().getName())) {
      List<RexNode> operands = call.getOperands();
      for (RexNode rexNode : operands) {
        Pair<Integer, Integer> pair = extractOneJoinColumn((RexCall) rexNode, leftRowColumnCount);
        pairs.add(pair);
      }
    } else if ("=".equals(call.getOperator().getName())) {
      pairs.add(extractOneJoinColumn(call, leftRowColumnCount));
    } else {
      throw new UnsupportedOperationException(
          "Operator " + call.getOperator().getName() + " is not supported in join condition");
    }

    return pairs;
  }

  private Pair<Integer, Integer> extractOneJoinColumn(
      RexCall oneCondition, int leftRowColumnCount) {
    List<RexNode> operands = oneCondition.getOperands();
    final int leftIndex =
        Math.min(
            ((RexInputRef) operands.get(0)).getIndex(), ((RexInputRef) operands.get(1)).getIndex());

    final int rightIndex1 =
        Math.max(
            ((RexInputRef) operands.get(0)).getIndex(), ((RexInputRef) operands.get(1)).getIndex());
    final int rightIndex = rightIndex1 - leftRowColumnCount;

    return new Pair<>(leftIndex, rightIndex);
  }

  private PCollection<Row> joinAsLookup(
      BeamRelNode leftRelNode, BeamRelNode rightRelNode, PCollection<Row> factStream) {
    BeamIOSourceRel srcRel = (BeamIOSourceRel) rightRelNode;
    BeamSqlSeekableTable seekableTable = (BeamSqlSeekableTable) srcRel.getBeamSqlTable();

    return factStream.apply(
        "join_as_lookup",
        new BeamJoinTransforms.JoinAsLookup(
            condition,
            seekableTable,
            CalciteUtils.toBeamSchema(rightRelNode.getRowType()),
            CalciteUtils.toBeamSchema(leftRelNode.getRowType()).getFieldCount()));
  }

  /** check if {@code BeamRelNode} implements {@code BeamSeekableTable}. */
  private boolean seekable(BeamRelNode relNode) {
    if (relNode instanceof BeamIOSourceRel) {
      BeamIOSourceRel srcRel = (BeamIOSourceRel) relNode;
      BeamSqlTable sourceTable = srcRel.getBeamSqlTable();
      if (sourceTable instanceof BeamSqlSeekableTable) {
        return true;
      }
    }
    return false;
  }
}
