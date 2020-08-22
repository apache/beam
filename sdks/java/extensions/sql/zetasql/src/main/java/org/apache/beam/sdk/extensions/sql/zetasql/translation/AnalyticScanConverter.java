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

import com.google.zetasql.resolvedast.ResolvedNode;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAnalyticScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedWindowFrameExpr;
import com.google.zetasql.resolvedast.ResolvedOrderByItemEnums;
import com.google.zetasql.resolvedast.ResolvedWindowFrameEnums;
import com.google.zetasql.resolvedast.ResolvedWindowFrameExprEnums;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.Lists;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamWindowRel;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlCalciteTranslationUtils;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollations;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Window;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Window.Group;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexWindowBound;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlAggFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlRankFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlWindow;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.ImmutableBitSet;

/** Converts AnalyticScan into BeamWindowRel. */
public class AnalyticScanConverter extends RelConverter<ResolvedAnalyticScan> {

  AnalyticScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public List<ResolvedNode> getInputs(ResolvedAnalyticScan zetaNode) {
    return Collections.singletonList(zetaNode.getInputScan());
  }

  @Override
  public RelNode convert(ResolvedAnalyticScan zetaNode, List<RelNode> inputs) {

    RelNode inputTable = inputs.get(0);

    RelDataType expectedRowType =
        getExpressionConverter().rexBuilder().getTypeFactory().copyType(inputTable.getRowType());

    List<RexLiteral> constants = Lists.newArrayList();
    List<Group> groups = Lists.newArrayList();

    int ordinal = 0;
    for (ResolvedNodes.ResolvedAnalyticFunctionGroup analyticGroup :
        zetaNode.getFunctionGroupList()) {
      ImmutableBitSet partitionKeys =
          ImmutableBitSet.of(
              analyticGroup.getPartitionBy() != null
                  ? analyticGroup.getPartitionBy().getPartitionByList().stream()
                      .map(
                          keyColumn -> {
                            return getExpressionConverter()
                                .indexOfProjectionColumnRef(
                                    keyColumn.getColumn().getId(), zetaNode.getColumnList());
                          })
                      .collect(Collectors.toList())
                  : Lists.newArrayList());
      RelCollation relCollation =
          RelCollations.of(
              analyticGroup.getOrderBy().getOrderByItemList().stream()
                  .map(
                      orderColumn -> {
                        int columnIndex =
                            getExpressionConverter()
                                .indexOfProjectionColumnRef(
                                    orderColumn.getColumnRef().getColumn().getId(),
                                    zetaNode.getColumnList());
                        RelFieldCollation.Direction columnDirection =
                            orderColumn.getIsDescending()
                                ? RelFieldCollation.Direction.DESCENDING
                                : RelFieldCollation.Direction.ASCENDING;
                        RelFieldCollation.NullDirection columnNull =
                            orderColumn.getNullOrder()
                                    == ResolvedOrderByItemEnums.NullOrderMode.NULLS_FIRST
                                ? RelFieldCollation.NullDirection.FIRST
                                : orderColumn.getNullOrder()
                                        == ResolvedOrderByItemEnums.NullOrderMode.NULLS_LAST
                                    ? RelFieldCollation.NullDirection.LAST
                                    : RelFieldCollation.NullDirection.UNSPECIFIED;
                        return new RelFieldCollation(columnIndex, columnDirection, columnNull);
                      })
                  .collect(Collectors.toList()));

      for (ResolvedComputedColumn aggColumn : analyticGroup.getAnalyticFunctionList()) {
        ResolvedAnalyticFunctionCall aggCall = (ResolvedAnalyticFunctionCall) aggColumn.getExpr();

        if (aggCall.getDistinct()) {
          throw new UnsupportedOperationException("Does not support DISTINTC");
        }

        SqlAggFunction sqlAggFunction =
            (SqlAggFunction)
                SqlOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(
                    aggCall.getFunction().getName());
        if (sqlAggFunction == null) {
          throw new UnsupportedOperationException(
              "Does not support ZetaSQL aggregate function: " + aggCall.getFunction().getName());
        }

        RelDataType columnReturnType =
            ZetaSqlCalciteTranslationUtils.toCalciteType(
                aggColumn.getColumn().getType(), true, getCluster().getRexBuilder());

        List<RexNode> argsColumns =
            aggCall.getArgumentList().stream()
                .map(
                    argument -> {
                      return getExpressionConverter().convertRexNodeFromResolvedExpr(argument);
                    })
                .collect(Collectors.toList());

        RexWinAggCall rexWinAggCall =
            new RexWinAggCall(
                sqlAggFunction,
                columnReturnType,
                argsColumns,
                ordinal++,
                aggCall.getDistinct(),
                false);

        RexNode rexPreceding = null;
        RexNode rexFollowing = null;

        if (aggCall.getWindowFrame() != null
            && (aggCall.getWindowFrame().getStartExpr().getBoundaryType()
                    == ResolvedWindowFrameExprEnums.BoundaryType.OFFSET_PRECEDING
                || aggCall.getWindowFrame().getStartExpr().getBoundaryType()
                    == ResolvedWindowFrameExprEnums.BoundaryType.OFFSET_FOLLOWING)) {
          constants.add(
              RexLiteral.fromJdbcString(
                  SqlOperators.BIGINT,
                  SqlTypeName.DECIMAL,
                  obtainValue(aggCall.getWindowFrame().getStartExpr()).toString()));

          rexPreceding =
              getExpressionConverter()
                  .rexBuilder()
                  .makeCall(
                      aggCall.getWindowFrame().getStartExpr().getBoundaryType()
                              == ResolvedWindowFrameExprEnums.BoundaryType.OFFSET_PRECEDING
                          ? SqlWindow.PRECEDING_OPERATOR
                          : SqlWindow.FOLLOWING_OPERATOR,
                      new RexInputRef(
                          inputTable.getRowType().getFieldCount() + constants.size() - 1,
                          SqlOperators.BIGINT));
        }
        if (aggCall.getWindowFrame() != null
            && (aggCall.getWindowFrame().getEndExpr().getBoundaryType()
                    == ResolvedWindowFrameExprEnums.BoundaryType.OFFSET_PRECEDING
                || aggCall.getWindowFrame().getEndExpr().getBoundaryType()
                    == ResolvedWindowFrameExprEnums.BoundaryType.OFFSET_FOLLOWING)) {
          constants.add(
              RexLiteral.fromJdbcString(
                  SqlOperators.BIGINT,
                  SqlTypeName.DECIMAL,
                  obtainValue(aggCall.getWindowFrame().getStartExpr()).toString()));

          rexFollowing =
              getExpressionConverter()
                  .rexBuilder()
                  .makeCall(
                      aggCall.getWindowFrame().getEndExpr().getBoundaryType()
                              == ResolvedWindowFrameExprEnums.BoundaryType.OFFSET_FOLLOWING
                          ? SqlWindow.FOLLOWING_OPERATOR
                          : SqlWindow.PRECEDING_OPERATOR,
                      new RexInputRef(
                          inputTable.getRowType().getFieldCount() + constants.size() - 1,
                          SqlOperators.BIGINT));
        }
        RexWindowBound rexWindowPreceding =
            RexWindowBound.create(
                aggCall.getWindowFrame() != null
                    ? obtainNode(aggCall.getWindowFrame().getStartExpr())
                    : SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO),
                rexPreceding);

        // Numbering functions use currentRow instead of unboundedFollowing in Calcite
        boolean isNumbering = sqlAggFunction instanceof SqlRankFunction;
        RexWindowBound rexWindowFollowing =
            RexWindowBound.create(
                aggCall.getWindowFrame() != null
                    ? obtainNode(aggCall.getWindowFrame().getEndExpr())
                    : isNumbering
                        ? SqlWindow.createCurrentRow(SqlParserPos.ZERO)
                        : SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO),
                rexFollowing);

        groups.add(
            new Window.Group(
                partitionKeys,
                aggCall.getWindowFrame() != null
                    ? aggCall.getWindowFrame().getFrameUnit()
                        == ResolvedWindowFrameEnums.FrameUnit.ROWS
                    : false,
                rexWindowPreceding,
                rexWindowFollowing,
                relCollation,
                Collections.singletonList(rexWinAggCall)));
        String aggColumnName = getTrait().resolveAlias(aggColumn.getColumn());
        expectedRowType =
            getExpressionConverter()
                .rexBuilder()
                .getTypeFactory()
                .builder()
                .addAll(expectedRowType.getFieldList())
                .add(aggColumnName, columnReturnType)
                .build();
      }
    }
    return new BeamWindowRel(
        inputTable.getCluster(),
        inputTable.getTraitSet(),
        inputTable,
        constants,
        expectedRowType,
        groups);
  }

  private SqlNode obtainNode(ResolvedWindowFrameExpr exp) {
    SqlNode st = null;
    switch (exp.getBoundaryType()) {
      case CURRENT_ROW:
        st = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
        break;
      case OFFSET_FOLLOWING:
        st =
            SqlWindow.createFollowing(
                SqlWindow.createBound(
                    SqlLiteral.createExactNumeric(obtainValue(exp).toString(), SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        break;
      case OFFSET_PRECEDING:
        st =
            SqlWindow.createPreceding(
                SqlWindow.createBound(
                    SqlLiteral.createExactNumeric(obtainValue(exp).toString(), SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        break;
      case UNBOUNDED_FOLLOWING:
        st = SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
        break;
      case UNBOUNDED_PRECEDING:
        st = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
        break;
    }
    return st;
  }

  private BigDecimal obtainValue(ResolvedWindowFrameExpr exp) {
    ResolvedLiteral of = (ResolvedLiteral) exp.getExpression();
    return new BigDecimal(of.getValue().getInt64Value());
  }
}
