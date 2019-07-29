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

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.joda.time.Duration.ZERO;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
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
    if (isSideInputLookupJoin()) {
      return ImmutableList.of(
          BeamSqlRelUtils.getBeamRelInput(getInputs().get(nonSeekableInputIndex().get())));
    } else {
      return BeamRelNode.super.getPCollectionInputs();
    }
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return (BeamCostModel) this.computeSelfCost(planner, mq);
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    double selectivity = mq.getSelectivity(this, getCondition());
    NodeStats leftEstimates = BeamSqlRelUtils.getNodeStats(this.left, mq);
    NodeStats rightEstimates = BeamSqlRelUtils.getNodeStats(this.right, mq);

    if (leftEstimates.isUnknown() || rightEstimates.isUnknown()) {
      return NodeStats.UNKNOWN;
    }
    // If any of the inputs are unbounded row count becomes zero (one of them would be zero)
    // If one is bounded and one unbounded the rate will be window of the bounded (= its row count)
    // multiplied by the rate of the unbounded one
    // If both are unbounded, the rate will be multiplication of each rate into the window of the
    // other.
    return NodeStats.create(
        leftEstimates.getRowCount() * rightEstimates.getRowCount() * selectivity,
        (leftEstimates.getRate() * rightEstimates.getWindow()
                + rightEstimates.getRate() * leftEstimates.getWindow())
            * selectivity,
        leftEstimates.getWindow() * rightEstimates.getWindow() * selectivity);
  }

  /**
   * This method checks if a join is legal and can be converted into Beam SQL. It is used during
   * planning and applying {@link
   * org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinAssociateRule} and {@link
   * org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinPushThroughJoinRule}
   */
  public static boolean isJoinLegal(Join join) {
    try {
      extractJoinRexNodes(join.getCondition());
    } catch (UnsupportedOperationException e) {
      return false;
    }
    return true;
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    if (isSideInputLookupJoin()) {
      return new SideInputLookupJoin();
    } else if (isSideInputJoin()) {
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

      BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
      BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);

      if ((joinType == JoinRelType.LEFT && leftRelNode.isBounded() == PCollection.IsBounded.BOUNDED)
          || (joinType == JoinRelType.RIGHT
              && rightRelNode.isBounded() == PCollection.IsBounded.BOUNDED)) {
        throw new UnsupportedOperationException(
            "LEFT side of an OUTER JOIN must be Unbounded table.");
      }

      return new SideInputJoin();
    } else {
      return new StandardJoin();
    }
  }

  private boolean isSideInputJoin() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    return (leftRelNode.isBounded() == PCollection.IsBounded.BOUNDED
            && rightRelNode.isBounded() == UNBOUNDED)
        || (leftRelNode.isBounded() == UNBOUNDED
            && rightRelNode.isBounded() == PCollection.IsBounded.BOUNDED);
  }

  private boolean isSideInputLookupJoin() {
    return seekableInputIndex().isPresent() && nonSeekableInputIndex().isPresent();
  }

  private Optional<Integer> seekableInputIndex() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    return seekable(leftRelNode)
        ? Optional.of(0)
        : seekable(rightRelNode) ? Optional.of(1) : Optional.absent();
  }

  private Optional<Integer> nonSeekableInputIndex() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    return !seekable(leftRelNode)
        ? Optional.of(0)
        : !seekable(rightRelNode) ? Optional.of(1) : Optional.absent();
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

  private class ExtractJoinKeys
      extends PTransform<PCollectionList<Row>, PCollectionList<KV<Row, Row>>> {

    @Override
    public PCollectionList<KV<Row, Row>> expand(PCollectionList<Row> pinput) {
      BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);

      Schema leftSchema = CalciteUtils.toSchema(left.getRowType());
      Schema rightSchema = CalciteUtils.toSchema(right.getRowType());

      assert pinput.size() == 2;
      PCollection<Row> leftRows = pinput.get(0);
      PCollection<Row> rightRows = pinput.get(1);

      int leftRowColumnCount = leftRelNode.getRowType().getFieldCount();

      // extract the join fields
      List<Pair<RexNode, RexNode>> pairs = extractJoinRexNodes(condition);

      // build the extract key type
      // the name of the join field is not important
      Schema extractKeySchemaLeft =
          pairs.stream()
              .map(pair -> getFieldBasedOnRexNode(leftSchema, pair.getKey(), 0))
              .collect(toSchema());
      Schema extractKeySchemaRight =
          pairs.stream()
              .map(pair -> getFieldBasedOnRexNode(rightSchema, pair.getValue(), leftRowColumnCount))
              .collect(toSchema());

      SchemaCoder<Row> extractKeyRowCoder = SchemaCoder.of(extractKeySchemaLeft);

      // BeamSqlRow -> KV<BeamSqlRow, BeamSqlRow>
      PCollection<KV<Row, Row>> extractedLeftRows =
          leftRows
              .apply(
                  "left_TimestampCombiner",
                  Window.<Row>configure().withTimestampCombiner(TimestampCombiner.EARLIEST))
              .apply(
                  "left_ExtractJoinFields",
                  MapElements.via(
                      new BeamJoinTransforms.ExtractJoinFields(
                          true, pairs, extractKeySchemaLeft, 0)))
              .setCoder(KvCoder.of(extractKeyRowCoder, leftRows.getCoder()));

      PCollection<KV<Row, Row>> extractedRightRows =
          rightRows
              .apply(
                  "right_TimestampCombiner",
                  Window.<Row>configure().withTimestampCombiner(TimestampCombiner.EARLIEST))
              .apply(
                  "right_ExtractJoinFields",
                  MapElements.via(
                      new BeamJoinTransforms.ExtractJoinFields(
                          false, pairs, extractKeySchemaRight, leftRowColumnCount)))
              .setCoder(KvCoder.of(extractKeyRowCoder, rightRows.getCoder()));

      return PCollectionList.of(extractedLeftRows).and(extractedRightRows);
    }
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

  private class StandardJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      Schema leftSchema = CalciteUtils.toSchema(left.getRowType());
      Schema rightSchema = CalciteUtils.toSchema(right.getRowType());

      PCollectionList<KV<Row, Row>> keyedInputs = pinput.apply(new ExtractJoinKeys());

      PCollection<KV<Row, Row>> extractedLeftRows = keyedInputs.get(0);
      PCollection<KV<Row, Row>> extractedRightRows = keyedInputs.get(1);

      WindowFn leftWinFn = extractedLeftRows.getWindowingStrategy().getWindowFn();
      WindowFn rightWinFn = extractedRightRows.getWindowingStrategy().getWindowFn();

      try {
        leftWinFn.verifyCompatibility(rightWinFn);
      } catch (IncompatibleWindowException e) {
        throw new IllegalArgumentException(
            "WindowFns must match for a bounded-vs-bounded/unbounded-vs-unbounded join.", e);
      }

      verifySupportedTrigger(extractedLeftRows);
      verifySupportedTrigger(extractedRightRows);

      return standardJoin(extractedLeftRows, extractedRightRows, leftSchema, rightSchema);
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
      PCollection<KV<Row, Row>> extractedLeftRows,
      PCollection<KV<Row, Row>> extractedRightRows,
      Schema leftSchema,
      Schema rightSchema) {
    PCollection<KV<Row, KV<Row, Row>>> joinedRows = null;

    switch (joinType) {
      case LEFT:
        {
          Schema rigthNullSchema = buildNullSchema(rightSchema);
          Row rightNullRow = Row.nullRow(rigthNullSchema);

          extractedRightRows = setValueCoder(extractedRightRows, SchemaCoder.of(rigthNullSchema));

          joinedRows =
              org.apache.beam.sdk.extensions.joinlibrary.Join.leftOuterJoin(
                  extractedLeftRows, extractedRightRows, rightNullRow);

          break;
        }
      case RIGHT:
        {
          Schema leftNullSchema = buildNullSchema(leftSchema);
          Row leftNullRow = Row.nullRow(leftNullSchema);

          extractedLeftRows = setValueCoder(extractedLeftRows, SchemaCoder.of(leftNullSchema));

          joinedRows =
              org.apache.beam.sdk.extensions.joinlibrary.Join.rightOuterJoin(
                  extractedLeftRows, extractedRightRows, leftNullRow);
          break;
        }
      case FULL:
        {
          Schema leftNullSchema = buildNullSchema(leftSchema);
          Schema rightNullSchema = buildNullSchema(rightSchema);

          Row leftNullRow = Row.nullRow(leftNullSchema);
          Row rightNullRow = Row.nullRow(rightNullSchema);

          extractedLeftRows = setValueCoder(extractedLeftRows, SchemaCoder.of(leftNullSchema));
          extractedRightRows = setValueCoder(extractedRightRows, SchemaCoder.of(rightNullSchema));

          joinedRows =
              org.apache.beam.sdk.extensions.joinlibrary.Join.fullOuterJoin(
                  extractedLeftRows, extractedRightRows, leftNullRow, rightNullRow);
          break;
        }
      case INNER:
      default:
        joinedRows =
            org.apache.beam.sdk.extensions.joinlibrary.Join.innerJoin(
                extractedLeftRows, extractedRightRows);
        break;
    }

    Schema schema = CalciteUtils.toSchema(getRowType());
    return joinedRows
        .apply(
            "JoinParts2WholeRow",
            MapElements.via(new BeamJoinTransforms.JoinParts2WholeRow(schema)))
        .setRowSchema(schema);
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

      realRightRows = setValueCoder(realRightRows, SchemaCoder.of(leftNullSchema));
      realRightNullRow = Row.nullRow(leftNullSchema);
    } else {
      Schema rightNullSchema = buildNullSchema(rightSchema);

      realRightRows = setValueCoder(realRightRows, SchemaCoder.of(rightNullSchema));
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

  private Schema buildNullSchema(Schema schema) {
    Schema.Builder builder = Schema.builder();

    builder.addFields(
        schema.getFields().stream().map(f -> f.withNullable(true)).collect(Collectors.toList()));

    return builder.build();
  }

  private static <K, V> PCollection<KV<K, V>> setValueCoder(
      PCollection<KV<K, V>> kvs, Coder<V> valueCoder) {
    // safe case because PCollection of KV always has KvCoder
    KvCoder<K, V> coder = (KvCoder<K, V>) kvs.getCoder();

    return kvs.setCoder(KvCoder.of(coder.getKeyCoder(), valueCoder));
  }

  private static Field getFieldBasedOnRexNode(
      Schema schema, RexNode rexNode, int leftRowColumnCount) {
    if (rexNode instanceof RexInputRef) {
      return schema.getField(((RexInputRef) rexNode).getIndex() - leftRowColumnCount);
    } else if (rexNode instanceof RexFieldAccess) {
      // need to extract field of Struct/Row.
      return getFieldBasedOnRexFieldAccess(schema, (RexFieldAccess) rexNode, leftRowColumnCount);
    }

    throw new UnsupportedOperationException("Does not support " + rexNode.getType() + " in JOIN.");
  }

  private static Field getFieldBasedOnRexFieldAccess(
      Schema schema, RexFieldAccess rexFieldAccess, int leftRowColumnCount) {
    ArrayDeque<RexFieldAccess> fieldAccessStack = new ArrayDeque<>();
    fieldAccessStack.push(rexFieldAccess);

    RexFieldAccess curr = rexFieldAccess;
    while (curr.getReferenceExpr() instanceof RexFieldAccess) {
      curr = (RexFieldAccess) curr.getReferenceExpr();
      fieldAccessStack.push(curr);
    }

    // curr.getReferenceExpr() is not a RexFieldAccess. Check if it is RexInputRef, which is only
    // allowed RexNode type in RexFieldAccess.
    if (!(curr.getReferenceExpr() instanceof RexInputRef)) {
      throw new UnsupportedOperationException(
          "Does not support " + curr.getReferenceExpr().getType() + " in JOIN.");
    }

    // curr.getReferenceExpr() is a RexInputRef.
    RexInputRef inputRef = (RexInputRef) curr.getReferenceExpr();
    Field curField = schema.getField(inputRef.getIndex() - leftRowColumnCount);

    // pop RexFieldAccess from stack one by one to know the final field type.
    while (fieldAccessStack.size() > 0) {
      curr = fieldAccessStack.pop();
      curField = curField.getType().getRowSchema().getField(curr.getField().getIndex());
    }

    return curField;
  }

  static List<Pair<RexNode, RexNode>> extractJoinRexNodes(RexNode condition) {
    // it's a CROSS JOIN because: condition == true
    if (condition instanceof RexLiteral && (Boolean) ((RexLiteral) condition).getValue()) {
      throw new UnsupportedOperationException("CROSS JOIN is not supported!");
    }

    RexCall call = (RexCall) condition;
    List<Pair<RexNode, RexNode>> pairs = new ArrayList<>();
    if ("AND".equals(call.getOperator().getName())) {
      List<RexNode> operands = call.getOperands();
      for (RexNode rexNode : operands) {
        Pair<RexNode, RexNode> pair = extractJoinPairOfRexNodes((RexCall) rexNode);
        pairs.add(pair);
      }
    } else if ("=".equals(call.getOperator().getName())) {
      pairs.add(extractJoinPairOfRexNodes(call));
    } else {
      throw new UnsupportedOperationException(
          "Operator " + call.getOperator().getName() + " is not supported in join condition");
    }

    return pairs;
  }

  private static Pair<RexNode, RexNode> extractJoinPairOfRexNodes(RexCall rexCall) {
    if (!rexCall.getOperator().getName().equals("=")) {
      throw new UnsupportedOperationException("Non equi-join is not supported");
    }

    if (isIllegalJoinConjunctionClause(rexCall)) {
      throw new UnsupportedOperationException(
          "Only support column reference or struct field access in conjunction clause");
    }

    int leftIndex = getColumnIndex(rexCall.getOperands().get(0));
    int rightIndex = getColumnIndex(rexCall.getOperands().get(1));
    if (leftIndex < rightIndex) {
      return new Pair<>(rexCall.getOperands().get(0), rexCall.getOperands().get(1));
    } else {
      return new Pair<>(rexCall.getOperands().get(1), rexCall.getOperands().get(0));
    }
  }

  // Only support {RexInputRef | RexFieldAccess} = {RexInputRef | RexFieldAccess}
  private static boolean isIllegalJoinConjunctionClause(RexCall rexCall) {
    return (!(rexCall.getOperands().get(0) instanceof RexInputRef)
            && !(rexCall.getOperands().get(0) instanceof RexFieldAccess))
        || (!(rexCall.getOperands().get(1) instanceof RexInputRef)
            && !(rexCall.getOperands().get(1) instanceof RexFieldAccess));
  }

  private static int getColumnIndex(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return ((RexInputRef) rexNode).getIndex();
    } else if (rexNode instanceof RexFieldAccess) {
      return getColumnIndex(((RexFieldAccess) rexNode).getReferenceExpr());
    }

    throw new UnsupportedOperationException("Cannot get column index from " + rexNode.getType());
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
