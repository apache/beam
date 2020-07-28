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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_CAST;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_COLUMN_REF;
import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_GET_STRUCT_FIELD;
import static org.apache.beam.sdk.extensions.sql.zetasql.SqlAnalyzer.USER_DEFINED_JAVA_AGGREGATE_FUNCTIONS;
import static org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlCalciteTranslationUtils.toCalciteTypeName;
import static org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlCalciteTranslationUtils.toSimpleRelDataType;
import static org.apache.beam.sdk.extensions.sql.zetasql.translation.SqlOperators.createUdafOperator;

import com.google.zetasql.FunctionSignature;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollations;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.AggregateCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlAggFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Converts aggregate calls. */
class AggregateScanConverter extends RelConverter<ResolvedAggregateScan> {
  private static final String AVG_ILLEGAL_LONG_INPUT_TYPE =
      "AVG(LONG) is not supported. You might want to use AVG(CAST(expression AS DOUBLE).";

  AggregateScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedAggregateScan zetaNode) {
    return Collections.singletonList(zetaNode.getInputScan());
  }

  @Override
  public RelNode convert(ResolvedAggregateScan zetaNode, List<RelNode> inputs) {
    LogicalProject input = convertAggregateScanInputScanToLogicalProject(zetaNode, inputs.get(0));

    // Calcite LogicalAggregate's GroupSet is indexes of group fields starting from 0.
    int groupFieldsListSize = zetaNode.getGroupByList().size();
    ImmutableBitSet groupSet;
    if (groupFieldsListSize != 0) {
      groupSet =
          ImmutableBitSet.of(
              IntStream.rangeClosed(0, groupFieldsListSize - 1)
                  .boxed()
                  .collect(Collectors.toList()));
    } else {
      groupSet = ImmutableBitSet.of();
    }

    // TODO: add support for indicator

    List<AggregateCall> aggregateCalls;
    if (zetaNode.getAggregateList().isEmpty()) {
      aggregateCalls = ImmutableList.of();
    } else {
      aggregateCalls = new ArrayList<>();
      // For aggregate calls, their input ref follow after GROUP BY input ref.
      int columnRefoff = groupFieldsListSize;
      boolean nullable = false;
      if (input.getProjects().size() > columnRefoff) {
        nullable = input.getProjects().get(columnRefoff).getType().isNullable();
      }
      for (ResolvedComputedColumn computedColumn : zetaNode.getAggregateList()) {
        AggregateCall aggCall = convertAggCall(computedColumn, columnRefoff, nullable);
        aggregateCalls.add(aggCall);
        if (!aggCall.getArgList().isEmpty()) {
          // Only increment column reference offset when aggregates use them (BEAM-8042).
          // Ex: COUNT(*) does not have arguments, while COUNT(`field`) does.
          columnRefoff++;
        }
      }
    }

    LogicalAggregate logicalAggregate =
        new LogicalAggregate(
            getCluster(),
            input.getTraitSet(),
            input,
            groupSet,
            ImmutableList.of(groupSet),
            aggregateCalls);

    return logicalAggregate;
  }

  private LogicalProject convertAggregateScanInputScanToLogicalProject(
      ResolvedAggregateScan node, RelNode input) {
    // AggregateScan's input is the source of data (e.g. TableScan), which is different from the
    // design of CalciteSQL, in which the LogicalAggregate's input is a LogicalProject, whose input
    // is a LogicalTableScan. When AggregateScan's input is WithRefScan, the WithRefScan is
    // ebullient to a LogicalTableScan. So it's still required to build another LogicalProject as
    // the input of LogicalAggregate.
    List<RexNode> projects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    // LogicalProject has a list of expr, which including UDF in GROUP BY clause for
    // LogicalAggregate.
    for (ResolvedComputedColumn computedColumn : node.getGroupByList()) {
      projects.add(
          getExpressionConverter()
              .convertRexNodeFromResolvedExpr(
                  computedColumn.getExpr(),
                  node.getInputScan().getColumnList(),
                  input.getRowType().getFieldList(),
                  ImmutableMap.of()));
      fieldNames.add(getTrait().resolveAlias(computedColumn.getColumn()));
    }

    // LogicalProject should also include columns used by aggregate functions. These columns should
    // follow after GROUP BY columns.
    // TODO: remove duplicate columns in projects.
    for (ResolvedComputedColumn resolvedComputedColumn : node.getAggregateList()) {
      // Should create Calcite's RexInputRef from ResolvedColumn from ResolvedComputedColumn.
      // TODO: handle aggregate function with more than one argument and handle OVER
      // TODO: is there is general way for column reference tracking and deduplication for
      // aggregation?
      ResolvedAggregateFunctionCall aggregateFunctionCall =
          ((ResolvedAggregateFunctionCall) resolvedComputedColumn.getExpr());
      if (aggregateFunctionCall.getArgumentList() != null
          && aggregateFunctionCall.getArgumentList().size() == 1) {
        ResolvedExpr resolvedExpr = aggregateFunctionCall.getArgumentList().get(0);

        // TODO: assume aggregate function's input is either a ColumnRef or a cast(ColumnRef).
        // TODO: user might use multiple CAST so we need to handle this rare case.
        projects.add(
            getExpressionConverter()
                .convertRexNodeFromResolvedExpr(
                    resolvedExpr,
                    node.getInputScan().getColumnList(),
                    input.getRowType().getFieldList(),
                    ImmutableMap.of()));
        fieldNames.add(getTrait().resolveAlias(resolvedComputedColumn.getColumn()));
      } else if (aggregateFunctionCall.getArgumentList() != null
          && aggregateFunctionCall.getArgumentList().size() > 1) {
        throw new IllegalArgumentException(
            aggregateFunctionCall.getFunction().getName() + " has more than one argument.");
      }
    }

    return LogicalProject.create(input, projects, fieldNames);
  }

  private AggregateCall convertAggCall(
      ResolvedComputedColumn computedColumn, int columnRefOff, boolean nullable) {
    ResolvedAggregateFunctionCall aggregateFunctionCall =
        (ResolvedAggregateFunctionCall) computedColumn.getExpr();

    // Reject AVG(INT64)
    if (aggregateFunctionCall.getFunction().getName().equals("avg")) {
      FunctionSignature signature = aggregateFunctionCall.getSignature();
      if (signature
          .getFunctionArgumentList()
          .get(0)
          .getType()
          .getKind()
          .equals(TypeKind.TYPE_INT64)) {
        throw new UnsupportedOperationException(AVG_ILLEGAL_LONG_INPUT_TYPE);
      }
    }

    // Reject aggregation DISTINCT
    if (aggregateFunctionCall.getDistinct()) {
      throw new UnsupportedOperationException(
          "Does not support "
              + aggregateFunctionCall.getFunction().getSqlName()
              + " DISTINCT. 'SELECT DISTINCT' syntax could be used to deduplicate before"
              + " aggregation.");
    }

    SqlAggFunction sqlAggFunction;
    if (aggregateFunctionCall
        .getFunction()
        .getGroup()
        .equals(USER_DEFINED_JAVA_AGGREGATE_FUNCTIONS)) {
      sqlAggFunction =
          createUdafOperator(
              aggregateFunctionCall.getFunction().getName(),
              x ->
                  createTypeFactory()
                      .createSqlType(
                          // TODO: is there a short way to find the return type of aggregation
                          // function?
                          toCalciteTypeName(
                              aggregateFunctionCall
                                  .getFunction()
                                  .getSignatureList()
                                  .get(0)
                                  .getResultType()
                                  .getType()
                                  .getKind())),
              new UdafImpl<>(Count.combineFn()));
    } else {
      sqlAggFunction =
          (SqlAggFunction)
              SqlOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(
                  aggregateFunctionCall.getFunction().getName());

      if (sqlAggFunction == null) {
        throw new UnsupportedOperationException(
            "Does not support ZetaSQL aggregate function: "
                + aggregateFunctionCall.getFunction().getName());
      }
    }

    List<Integer> argList = new ArrayList<>();
    for (ResolvedExpr expr :
        ((ResolvedAggregateFunctionCall) computedColumn.getExpr()).getArgumentList()) {
      // Throw an error if aggregate function's input isn't either a ColumnRef or a cast(ColumnRef).
      // TODO: is there a general way to handle aggregation calls conversion?
      if (expr.nodeKind() == RESOLVED_CAST
          || expr.nodeKind() == RESOLVED_COLUMN_REF
          || expr.nodeKind() == RESOLVED_GET_STRUCT_FIELD) {
        argList.add(columnRefOff);
      } else {
        throw new UnsupportedOperationException(
            "Aggregate function only accepts Column Reference or CAST(Column Reference) as its"
                + " input.");
      }
    }

    RelDataType returnType =
        toSimpleRelDataType(
            computedColumn.getColumn().getType().getKind(), getCluster().getRexBuilder(), nullable);

    String aggName = getTrait().resolveAlias(computedColumn.getColumn());
    return AggregateCall.create(
        sqlAggFunction, false, false, false, argList, -1, RelCollations.EMPTY, returnType, aggName);
  }

  private static RelDataTypeFactory createTypeFactory() {
    return new SqlTypeFactoryImpl(BeamRelDataTypeSystem.INSTANCE);
  }
}
