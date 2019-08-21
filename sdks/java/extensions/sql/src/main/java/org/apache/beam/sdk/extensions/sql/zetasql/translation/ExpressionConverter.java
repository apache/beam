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

import static com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind.RESOLVED_FUNCTION_CALL;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_BOOL;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_BYTES;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_DOUBLE;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_INT64;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_STRING;
import static com.google.zetasql.ZetaSQLType.TypeKind.TYPE_TIMESTAMP;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.convertDateValueToDateString;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.convertTimeValueToTimeString;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.safeMicrosToMillis;
import static org.apache.beam.sdk.extensions.sql.zetasql.SqlStdOperatorMappingTable.FUNCTION_FAMILY_DATE_ADD;
import static org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLCastFunctionImpl.ZETASQL_CAST_OP;

import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.ArrayType;
import com.google.zetasql.Type;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.functions.ZetaSQLDateTime.DateTimestampPart;
import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedAggregateScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedCast;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedComputedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFunctionCall;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedGetStructField;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedOrderByScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedParameter;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedProjectScan;
import io.grpc.Status;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.zetasql.SqlOperatorRewriter;
import org.apache.beam.sdk.extensions.sql.zetasql.SqlOperators;
import org.apache.beam.sdk.extensions.sql.zetasql.SqlStdOperatorMappingTable;
import org.apache.beam.sdk.extensions.sql.zetasql.TypeUtils;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

/**
 * Extracts expressions (function calls, field accesses) from the resolve query nodes, converts them
 * to RexNodes.
 */
@Internal
public class ExpressionConverter {

  private static final String PRE_DEFINED_WINDOW_FUNCTIONS = "pre_defined_window_functions";

  // Constants of pre-defined functions.
  private static final String WINDOW_START = "_START";
  private static final String WINDOW_END = "_END";
  private static final String FIXED_WINDOW = "TUMBLE";
  private static final String FIXED_WINDOW_START = FIXED_WINDOW + WINDOW_START;
  private static final String FIXED_WINDOW_END = FIXED_WINDOW + WINDOW_END;
  private static final String SLIDING_WINDOW = "HOP";
  private static final String SLIDING_WINDOW_START = SLIDING_WINDOW + WINDOW_START;
  private static final String SLIDING_WINDOW_END = SLIDING_WINDOW + WINDOW_END;
  private static final String SESSION_WINDOW = "SESSION";
  private static final String SESSION_WINDOW_START = SESSION_WINDOW + WINDOW_START;
  private static final String SESSION_WINDOW_END = SESSION_WINDOW + WINDOW_END;

  private static final ImmutableMap<String, String> WINDOW_START_END_TO_WINDOW_MAP =
      ImmutableMap.<String, String>builder()
          .put(FIXED_WINDOW_START, FIXED_WINDOW)
          .put(FIXED_WINDOW_END, FIXED_WINDOW)
          .put(SLIDING_WINDOW_START, SLIDING_WINDOW)
          .put(SLIDING_WINDOW_END, SLIDING_WINDOW)
          .put(SESSION_WINDOW_START, SESSION_WINDOW)
          .put(SESSION_WINDOW_END, SESSION_WINDOW)
          .build();

  private static final ImmutableSet<String> WINDOW_START_END_FUNCTION_SET =
      ImmutableSet.of(
          FIXED_WINDOW_START,
          FIXED_WINDOW_END,
          SLIDING_WINDOW_START,
          SLIDING_WINDOW_END,
          SESSION_WINDOW_START,
          SESSION_WINDOW_END);

  private static final ImmutableMap<TypeKind, ImmutableSet<TypeKind>> UNSUPPORTED_CASTING =
      ImmutableMap.<TypeKind, ImmutableSet<TypeKind>>builder()
          .put(TYPE_INT64, ImmutableSet.of(TYPE_DOUBLE))
          .put(TYPE_BOOL, ImmutableSet.of(TYPE_STRING))
          .put(TYPE_STRING, ImmutableSet.of(TYPE_BOOL, TYPE_DOUBLE))
          .build();

  private static final ImmutableMap<Integer, TimeUnit> TIME_UNIT_CASTING_MAP =
      ImmutableMap.<Integer, TimeUnit>builder()
          .put(DateTimestampPart.YEAR.getNumber(), TimeUnit.YEAR)
          .put(DateTimestampPart.MONTH.getNumber(), TimeUnit.MONTH)
          .put(DateTimestampPart.DAY.getNumber(), TimeUnit.DAY)
          .put(DateTimestampPart.DAYOFWEEK.getNumber(), TimeUnit.DOW)
          .put(DateTimestampPart.DAYOFYEAR.getNumber(), TimeUnit.DOY)
          .put(DateTimestampPart.QUARTER.getNumber(), TimeUnit.QUARTER)
          .put(DateTimestampPart.HOUR.getNumber(), TimeUnit.HOUR)
          .put(DateTimestampPart.MINUTE.getNumber(), TimeUnit.MINUTE)
          .put(DateTimestampPart.SECOND.getNumber(), TimeUnit.SECOND)
          .put(DateTimestampPart.MILLISECOND.getNumber(), TimeUnit.MILLISECOND)
          .put(DateTimestampPart.MICROSECOND.getNumber(), TimeUnit.MICROSECOND)
          .put(DateTimestampPart.NANOSECOND.getNumber(), TimeUnit.NANOSECOND)
          .put(DateTimestampPart.ISOYEAR.getNumber(), TimeUnit.ISOYEAR)
          .put(DateTimestampPart.ISOWEEK.getNumber(), TimeUnit.WEEK)
          .build();

  private static final ImmutableSet<String> DATE_PART_UNITS_TO_MILLIS =
      ImmutableSet.of("DAY", "HOUR", "MINUTE", "SECOND");
  private static final ImmutableSet<String> DATE_PART_UNITS_TO_MONTHS = ImmutableSet.of("YEAR");

  private static final long ONE_SECOND_IN_MILLIS = 1000L;
  private static final long ONE_MINUTE_IN_MILLIS = 60L * ONE_SECOND_IN_MILLIS;
  private static final long ONE_HOUR_IN_MILLIS = 60L * ONE_MINUTE_IN_MILLIS;
  private static final long ONE_DAY_IN_MILLIS = 24L * ONE_HOUR_IN_MILLIS;

  @SuppressWarnings("unused")
  private static final long ONE_MONTH_IN_MILLIS = 30L * ONE_DAY_IN_MILLIS;

  @SuppressWarnings("unused")
  private static final long ONE_YEAR_IN_MILLIS = 365L * ONE_DAY_IN_MILLIS;

  // Constants of error messages.
  private static final String INTERVAL_DATE_PART_MSG =
      "YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND";
  private static final String INTERVAL_FORMAT_MSG =
      "INTERVAL should be set as a STRING in the specific format: \"INTERVAL int64 date_part\"."
          + " The date_part includes: "
          + INTERVAL_DATE_PART_MSG;

  private final RelOptCluster cluster;
  private final Map<String, Value> queryParams;

  public ExpressionConverter(RelOptCluster cluster, Map<String, Value> params) {
    this.cluster = cluster;
    this.queryParams = params;
  }

  /** Extract expressions from a project scan node. */
  public List<RexNode> retrieveRexNode(ResolvedProjectScan node, List<RelDataTypeField> fieldList) {
    List<RexNode> ret = new ArrayList<>();

    for (ResolvedColumn column : node.getColumnList()) {
      int index = -1;
      if ((index = indexOfResolvedColumnInExprList(node.getExprList(), column)) != -1) {
        ResolvedComputedColumn computedColumn = node.getExprList().get(index);
        int windowFieldIndex = -1;
        if (computedColumn.getExpr().nodeKind() == RESOLVED_FUNCTION_CALL) {
          String functionName =
              ((ResolvedFunctionCall) computedColumn.getExpr()).getFunction().getName();
          if (WINDOW_START_END_FUNCTION_SET.contains(functionName)) {
            ResolvedAggregateScan resolvedAggregateScan =
                (ResolvedAggregateScan) node.getInputScan();
            windowFieldIndex =
                indexOfWindowField(
                    resolvedAggregateScan.getGroupByList(),
                    resolvedAggregateScan.getColumnList(),
                    WINDOW_START_END_TO_WINDOW_MAP.get(functionName));
          }
        }
        ret.add(
            convertRexNodeFromComputedColumnWithFieldList(
                computedColumn, node.getInputScan().getColumnList(), fieldList, windowFieldIndex));
      } else {
        // ResolvedColumn is not a expression, which means it has to be an input column reference.
        index = indexOfProjectionColumnRef(column.getId(), node.getInputScan().getColumnList());
        if (index < 0 || index >= node.getInputScan().getColumnList().size()) {
          throw new RuntimeException(
              String.format("Cannot find %s in fieldList %s", column, fieldList));
        }

        ret.add(rexBuilder().makeInputRef(fieldList.get(index).getType(), index));
      }
    }
    return ret;
  }

  /** Extract expressions from order by scan node. */
  public List<RexNode> retrieveRexNodeFromOrderByScan(
      RelOptCluster cluster, ResolvedOrderByScan node, List<RelDataTypeField> fieldList) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    List<RexNode> ret = new ArrayList<>();

    for (ResolvedColumn column : node.getColumnList()) {
      int index = indexOfProjectionColumnRef(column.getId(), node.getInputScan().getColumnList());
      ret.add(rexBuilder.makeInputRef(fieldList.get(index).getType(), index));
    }

    return ret;
  }

  private static int indexOfResolvedColumnInExprList(
      ImmutableList<ResolvedComputedColumn> exprList, ResolvedColumn column) {
    if (exprList == null || exprList.isEmpty()) {
      return -1;
    }

    for (int i = 0; i < exprList.size(); i++) {
      ResolvedComputedColumn computedColumn = exprList.get(i);
      if (computedColumn.getColumn().equals(column)) {
        return i;
      }
    }

    return -1;
  }

  private static int indexOfWindowField(
      List<ResolvedComputedColumn> groupByList, List<ResolvedColumn> columnList, String windowFn) {
    for (ResolvedComputedColumn groupByComputedColumn : groupByList) {
      if (groupByComputedColumn.getExpr().nodeKind() == RESOLVED_FUNCTION_CALL) {
        ResolvedFunctionCall functionCall = (ResolvedFunctionCall) groupByComputedColumn.getExpr();
        if (functionCall.getFunction().getName().equals(windowFn)) {
          int ret =
              indexOfResolvedColumnInColumnList(columnList, groupByComputedColumn.getColumn());
          if (ret == -1) {
            throw new RuntimeException("Cannot find " + windowFn + " in " + groupByList);
          } else {
            return ret;
          }
        }
      }
    }

    throw new RuntimeException("Cannot find " + windowFn + " in " + groupByList);
  }

  private static int indexOfResolvedColumnInColumnList(
      List<ResolvedColumn> columnList, ResolvedColumn column) {
    if (columnList == null || columnList.isEmpty()) {
      return -1;
    }

    for (int i = 0; i < columnList.size(); i++) {
      if (columnList.get(i).equals(column)) {
        return i;
      }
    }

    return -1;
  }

  /** Create a RexNode for a corresponding resolved expression node. */
  public RexNode convertRexNodeFromResolvedExpr(
      ResolvedExpr expr, List<ResolvedColumn> columnList, List<RelDataTypeField> fieldList) {
    if (columnList == null || fieldList == null) {
      return convertRexNodeFromResolvedExpr(expr);
    }

    RexNode ret;

    switch (expr.nodeKind()) {
      case RESOLVED_LITERAL:
        ret = convertResolvedLiteral((ResolvedLiteral) expr);
        break;
      case RESOLVED_COLUMN_REF:
        ret = convertResolvedColumnRef((ResolvedColumnRef) expr, columnList, fieldList);
        break;
      case RESOLVED_FUNCTION_CALL:
        ret = convertResolvedFunctionCall((ResolvedFunctionCall) expr, columnList, fieldList);
        break;
      case RESOLVED_CAST:
        ret = convertResolvedCast((ResolvedCast) expr, columnList, fieldList);
        break;
      case RESOLVED_PARAMETER:
        ret = convertResolvedParameter((ResolvedParameter) expr);
        break;
      case RESOLVED_GET_STRUCT_FIELD:
        ret =
            convertResolvedStructFieldAccess((ResolvedGetStructField) expr, columnList, fieldList);
        break;
      default:
        ret = convertRexNodeFromResolvedExpr(expr);
    }

    return ret;
  }

  /** Create a RexNode for a corresponding resolved expression. */
  public RexNode convertRexNodeFromResolvedExpr(ResolvedExpr expr) {
    RexNode ret;

    switch (expr.nodeKind()) {
      case RESOLVED_LITERAL:
        ret = convertResolvedLiteral((ResolvedLiteral) expr);
        break;
      case RESOLVED_COLUMN_REF:
        ret = convertResolvedColumnRef((ResolvedColumnRef) expr);
        break;
      case RESOLVED_FUNCTION_CALL:
        // TODO: is there a better way to shared code for different cases of
        // convertResolvedFunctionCall than passing into two nulls?
        ret = convertResolvedFunctionCall((ResolvedFunctionCall) expr, null, null);
        break;
      case RESOLVED_CAST:
        ret = convertResolvedCast((ResolvedCast) expr, null, null);
        break;
      case RESOLVED_PARAMETER:
        ret = convertResolvedParameter((ResolvedParameter) expr);
        break;
      case RESOLVED_GET_STRUCT_FIELD:
        ret = convertResolvedStructFieldAccess((ResolvedGetStructField) expr);
        break;
      case RESOLVED_SUBQUERY_EXPR:
        throw new IllegalArgumentException("Does not support sub-queries");
      default:
        throw new RuntimeException("Does not support expr node kind " + expr.nodeKind());
    }

    return ret;
  }

  /** Extract the RexNode from expression with ref scan. */
  public RexNode convertRexNodeFromResolvedExprWithRefScan(
      ResolvedExpr expr,
      List<ResolvedColumn> refScanLeftColumnList,
      List<RelDataTypeField> leftFieldList,
      List<ResolvedColumn> originalLeftColumnList,
      List<ResolvedColumn> refScanRightColumnList,
      List<RelDataTypeField> rightFieldList,
      List<ResolvedColumn> originalRightColumnList) {
    RexNode ret;

    switch (expr.nodeKind()) {
      case RESOLVED_LITERAL:
        ret = convertResolvedLiteral((ResolvedLiteral) expr);
        break;
      case RESOLVED_COLUMN_REF:
        ResolvedColumnRef columnRef = (ResolvedColumnRef) expr;
        // first look for column ref on the left side
        ret =
            convertRexNodeFromResolvedColumnRefWithRefScan(
                columnRef, refScanLeftColumnList, originalLeftColumnList, leftFieldList);

        // if not found there look on the right
        if (ret == null) {
          ret =
              convertRexNodeFromResolvedColumnRefWithRefScan(
                  columnRef, refScanRightColumnList, originalRightColumnList, rightFieldList);
        }

        break;
      case RESOLVED_FUNCTION_CALL:
        // JOIN only support equal join.
        ResolvedFunctionCall resolvedFunctionCall = (ResolvedFunctionCall) expr;
        List<RexNode> operands = new ArrayList<>();

        for (ResolvedExpr resolvedExpr : resolvedFunctionCall.getArgumentList()) {
          operands.add(
              convertRexNodeFromResolvedExprWithRefScan(
                  resolvedExpr,
                  refScanLeftColumnList,
                  leftFieldList,
                  originalLeftColumnList,
                  refScanRightColumnList,
                  rightFieldList,
                  originalRightColumnList));
        }

        SqlOperator op =
            SqlStdOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(
                resolvedFunctionCall.getFunction().getName());
        ret = rexBuilder().makeCall(op, operands);
        break;
      case RESOLVED_CAST:
        ResolvedCast resolvedCast = (ResolvedCast) expr;
        RexNode operand =
            convertRexNodeFromResolvedExprWithRefScan(
                resolvedCast.getExpr(),
                refScanLeftColumnList,
                leftFieldList,
                originalLeftColumnList,
                refScanRightColumnList,
                rightFieldList,
                originalRightColumnList);

        TypeKind fromType = resolvedCast.getExpr().getType().getKind();
        TypeKind toType = resolvedCast.getType().getKind();
        isCastingSupported(fromType, toType);

        RelDataType outputType =
            TypeUtils.toSimpleRelDataType(toType, rexBuilder(), operand.getType().isNullable());

        if (isZetaSQLCast(fromType, toType)) {
          ret = rexBuilder().makeCall(outputType, ZETASQL_CAST_OP, ImmutableList.of(operand));
        } else {
          ret = rexBuilder().makeCast(outputType, operand);
        }
        break;
      default:
        throw new RuntimeException("Does not support expr node kind " + expr.nodeKind());
    }

    return ret;
  }

  private RexNode convertRexNodeFromComputedColumnWithFieldList(
      ResolvedComputedColumn column,
      List<ResolvedColumn> columnList,
      List<RelDataTypeField> fieldList,
      int windowFieldIndex) {
    if (column.getExpr().nodeKind() != RESOLVED_FUNCTION_CALL) {
      return convertRexNodeFromResolvedExpr(column.getExpr(), columnList, fieldList);
    }

    ResolvedFunctionCall functionCall = (ResolvedFunctionCall) column.getExpr();

    // TODO: is there any other illegal case?
    if (functionCall.getFunction().getName().equals(FIXED_WINDOW)
        || functionCall.getFunction().getName().equals(SLIDING_WINDOW)
        || functionCall.getFunction().getName().equals(SESSION_WINDOW)) {
      throw new RuntimeException(
          functionCall.getFunction().getName() + " shouldn't appear in SELECT exprlist.");
    }

    if (!functionCall.getFunction().getGroup().equals(PRE_DEFINED_WINDOW_FUNCTIONS)) {
      // non-window function should still go through normal FunctionCall conversion process.
      return convertRexNodeFromResolvedExpr(column.getExpr(), columnList, fieldList);
    }

    // ONLY window_start and window_end should arrive here.
    // TODO: Have extra verification here to make sure window start/end functions have the same
    // parameter with window function.
    List<RexNode> operands = new ArrayList<>();
    switch (functionCall.getFunction().getName()) {
      case FIXED_WINDOW_START:
      case SLIDING_WINDOW_START:
      case SESSION_WINDOW_START:
        // TODO: in Calcite implementation, session window's start is equal to end. Need to fix it
        // in Calcite.
      case SESSION_WINDOW_END:
        return rexBuilder()
            .makeInputRef(fieldList.get(windowFieldIndex).getType(), windowFieldIndex);
      case FIXED_WINDOW_END:
        // WINDOW END is a function call
        operands.add(
            rexBuilder().makeInputRef(fieldList.get(windowFieldIndex).getType(), windowFieldIndex));
        // TODO: check window_end 's duration is the same as it's aggregate window.
        operands.add(
            convertIntervalToRexIntervalLiteral(
                (ResolvedLiteral) functionCall.getArgumentList().get(0)));
        return rexBuilder().makeCall(SqlStdOperatorTable.PLUS, operands);
      case SLIDING_WINDOW_END:
        operands.add(
            rexBuilder().makeInputRef(fieldList.get(windowFieldIndex).getType(), windowFieldIndex));
        operands.add(
            convertIntervalToRexIntervalLiteral(
                (ResolvedLiteral) functionCall.getArgumentList().get(1)));
        return rexBuilder().makeCall(SqlStdOperatorTable.PLUS, operands);
      default:
        throw new RuntimeException(
            "Does not support window start/end: " + functionCall.getFunction().getName());
    }
  }

  /** Convert a resolved literal to a RexNode. */
  public RexNode convertResolvedLiteral(ResolvedLiteral resolvedLiteral) {
    TypeKind kind = resolvedLiteral.getType().getKind();
    RexNode ret;
    switch (kind) {
      case TYPE_BOOL:
      case TYPE_INT32:
      case TYPE_INT64:
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
      case TYPE_STRING:
      case TYPE_TIMESTAMP:
      case TYPE_DATE:
      case TYPE_TIME:
        // case TYPE_DATETIME:
      case TYPE_BYTES:
      case TYPE_ARRAY:
      case TYPE_STRUCT:
      case TYPE_ENUM:
        ret = convertValueToRexNode(resolvedLiteral.getType(), resolvedLiteral.getValue());
        break;
      default:
        throw new RuntimeException(
            MessageFormat.format(
                "Unsupported ResolvedLiteral type: {0}, kind: {1}, value: {2}, class: {3}",
                resolvedLiteral.getType().typeName(),
                kind,
                resolvedLiteral.getValue(),
                resolvedLiteral.getClass()));
    }

    return ret;
  }

  private RexNode convertValueToRexNode(Type type, Value value) {
    RexNode ret;
    switch (type.getKind()) {
      case TYPE_BOOL:
      case TYPE_INT32:
      case TYPE_INT64:
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
      case TYPE_STRING:
      case TYPE_TIMESTAMP:
      case TYPE_DATE:
      case TYPE_TIME:
        // case TYPE_DATETIME:
      case TYPE_BYTES:
        ret = convertSimpleValueToRexNode(type.getKind(), value);
        break;
      case TYPE_ARRAY:
        ret = convertArrayValueToRexNode(type.asArray(), value);
        break;
      case TYPE_ENUM:
        ret = convertEnumToRexNode(type, value);
        break;
      default:
        // TODO: convert struct literal.
        throw new RuntimeException(
            "Unsupported ResolvedLiteral kind: " + type.getKind() + " type: " + type.typeName());
    }

    return ret;
  }

  private RexNode convertSimpleValueToRexNode(TypeKind kind, Value value) {
    if (value.isNull()) {
      return rexBuilder().makeNullLiteral(TypeUtils.toSimpleRelDataType(kind, rexBuilder()));
    }

    RexNode ret;
    switch (kind) {
      case TYPE_BOOL:
        ret = rexBuilder().makeLiteral(value.getBoolValue());
        break;
      case TYPE_INT32:
        ret =
            rexBuilder()
                .makeExactLiteral(
                    new BigDecimal(value.getInt32Value()),
                    TypeUtils.toSimpleRelDataType(kind, rexBuilder()));
        break;
      case TYPE_INT64:
        ret =
            rexBuilder()
                .makeExactLiteral(
                    new BigDecimal(value.getInt64Value()),
                    TypeUtils.toSimpleRelDataType(kind, rexBuilder()));
        break;
      case TYPE_FLOAT:
        ret =
            rexBuilder()
                .makeApproxLiteral(
                    new BigDecimal(value.getFloatValue()),
                    TypeUtils.toSimpleRelDataType(kind, rexBuilder()));
        break;
      case TYPE_DOUBLE:
        ret =
            rexBuilder()
                .makeApproxLiteral(
                    new BigDecimal(value.getDoubleValue()),
                    TypeUtils.toSimpleRelDataType(kind, rexBuilder()));
        break;
      case TYPE_STRING:
        // has to allow CAST because Calcite create CHAR type first and does a CAST to VARCHAR.
        // If not allow cast, rexBuilder() will only build a literal with CHAR type.
        ret =
            rexBuilder()
                .makeLiteral(
                    value.getStringValue(), typeFactory().createSqlType(SqlTypeName.VARCHAR), true);
        break;
      case TYPE_TIMESTAMP:
        ret =
            rexBuilder()
                .makeTimestampLiteral(
                    TimestampString.fromMillisSinceEpoch(
                        safeMicrosToMillis(value.getTimestampUnixMicros())),
                    typeFactory().getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP));
        break;
      case TYPE_DATE:
        ret = rexBuilder().makeDateLiteral(convertDateValueToDateString(value));
        break;
      case TYPE_TIME:
        RelDataType timeType =
            typeFactory()
                .createSqlType(
                    SqlTypeName.TIME,
                    typeFactory().getTypeSystem().getMaxPrecision(SqlTypeName.TIME));
        // TODO: Doing micro to mills truncation, need to throw exception.
        ret = rexBuilder().makeLiteral(convertTimeValueToTimeString(value), timeType, false);
        break;
      case TYPE_BYTES:
        ret = rexBuilder().makeBinaryLiteral(new ByteString(value.getBytesValue().toByteArray()));
        break;
      default:
        throw new RuntimeException("Unsupported column type: " + kind);
    }

    return ret;
  }

  private RexNode convertArrayValueToRexNode(ArrayType arrayType, Value value) {
    if (value.isNull()) {
      // TODO: should the nullable be false for a array?
      return rexBuilder()
          .makeNullLiteral(TypeUtils.toArrayRelDataType(rexBuilder(), arrayType, false));
    }

    List<RexNode> operands = new ArrayList<>();
    for (Value v : value.getElementList()) {
      operands.add(convertValueToRexNode(arrayType.getElementType(), v));
    }
    return rexBuilder().makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands);
  }

  private RexNode convertEnumToRexNode(Type type, Value value) {
    if (type.typeName().equals("`zetasql.functions.DateTimestampPart`")) {
      return convertTimeUnitRangeEnumToRexNode(type, value);
    } else {
      throw new RuntimeException(
          MessageFormat.format(
              "Unsupported enum. Kind: {0} Type: {1}", type.getKind(), type.typeName()));
    }
  }

  private RexNode convertTimeUnitRangeEnumToRexNode(Type type, Value value) {
    TimeUnit mappedUnit = TIME_UNIT_CASTING_MAP.get(value.getEnumValue());
    if (mappedUnit == null) {
      throw new RuntimeException(
          MessageFormat.format(
              "Unsupported enum value. Kind: {0} Type: {1} Value: {2} EnumName: {3}",
              type.getKind(), type.typeName(), value.getEnumName(), value.getEnumValue()));
    }

    TimeUnitRange mappedRange = TimeUnitRange.of(mappedUnit, null);
    return rexBuilder().makeFlag(mappedRange);
  }

  private RexNode convertResolvedColumnRef(
      ResolvedColumnRef columnRef,
      List<ResolvedColumn> columnList,
      List<RelDataTypeField> fieldList) {
    int index = indexOfProjectionColumnRef(columnRef.getColumn().getId(), columnList);
    if (index < 0 || index >= columnList.size()) {
      throw new RuntimeException(
          String.format("Cannot find %s in fieldList %s", columnRef.getColumn(), fieldList));
    }
    return rexBuilder().makeInputRef(fieldList.get(index).getType(), index);
  }

  private RexNode convertResolvedColumnRef(ResolvedColumnRef columnRef) {
    // TODO: id - 1 might be only correct if the columns read from TableScan.
    // What if the columns come from other scans (which means their id are not indexed from 0),
    // and what if there are some mis-order?
    // TODO: can join key be NULL?
    return rexBuilder()
        .makeInputRef(
            TypeUtils.toRelDataType(rexBuilder(), columnRef.getType(), false),
            (int) columnRef.getColumn().getId() - 1);
  }

  /** Return an index of the projection column reference. */
  public int indexOfProjectionColumnRef(long colId, List<ResolvedColumn> columnList) {
    int ret = -1;
    for (int i = 0; i < columnList.size(); i++) {
      if (columnList.get(i).getId() == colId) {
        ret = i;
        break;
      }
    }

    return ret;
  }

  private RexNode convertResolvedFunctionCall(
      ResolvedFunctionCall functionCall,
      List<ResolvedColumn> columnList,
      List<RelDataTypeField> fieldList) {
    RexNode ret;
    SqlOperator op;
    List<RexNode> operands = new ArrayList<>();

    if (functionCall.getFunction().getGroup().equals(PRE_DEFINED_WINDOW_FUNCTIONS)) {
      switch (functionCall.getFunction().getName()) {
        case FIXED_WINDOW:
        case SESSION_WINDOW:
          op =
              SqlStdOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(
                  functionCall.getFunction().getName());
          // TODO: check size and type of window function argument list.
          // Add ts column reference to operands.
          operands.add(
              convertRexNodeFromResolvedExpr(
                  functionCall.getArgumentList().get(0), columnList, fieldList));
          // Add fixed window size or session window gap to operands.
          operands.add(
              convertIntervalToRexIntervalLiteral(
                  (ResolvedLiteral) functionCall.getArgumentList().get(1)));
          break;
        case SLIDING_WINDOW:
          op =
              SqlStdOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(
                  SLIDING_WINDOW);
          // Add ts column reference to operands.
          operands.add(
              convertRexNodeFromResolvedExpr(
                  functionCall.getArgumentList().get(0), columnList, fieldList));
          // add sliding window emit frequency to operands.
          operands.add(
              convertIntervalToRexIntervalLiteral(
                  (ResolvedLiteral) functionCall.getArgumentList().get(1)));
          // add sliding window size to operands.
          operands.add(
              convertIntervalToRexIntervalLiteral(
                  (ResolvedLiteral) functionCall.getArgumentList().get(2)));
          break;
        default:
          throw new RuntimeException("Only support TUMBLE, HOP AND SESSION functions right now.");
      }
    } else if (functionCall.getFunction().getGroup().equals("ZetaSQL")) {
      op =
          SqlStdOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR.get(
              functionCall.getFunction().getName());

      if (op == null) {
        throw new RuntimeException(
            "Does not support ZetaSQL function: " + functionCall.getFunction().getName());
      }

      // There are different processes to handle argument conversion because INTERVAL is not a
      // type in ZetaSQL.
      if (FUNCTION_FAMILY_DATE_ADD.contains(functionCall.getFunction().getName())) {
        return convertTimestampAddFunction(functionCall, columnList, fieldList);
      } else {
        for (ResolvedExpr expr : functionCall.getArgumentList()) {
          operands.add(convertRexNodeFromResolvedExpr(expr, columnList, fieldList));
        }
      }
    } else {
      throw new RuntimeException(
          "Does not support function group: " + functionCall.getFunction().getGroup());
    }

    SqlOperatorRewriter rewriter =
        SqlStdOperatorMappingTable.ZETASQL_FUNCTION_TO_CALCITE_SQL_OPERATOR_REWRITER.get(
            functionCall.getFunction().getName());

    if (rewriter != null) {
      ret = rewriter.apply(rexBuilder(), operands);
    } else {
      ret = rexBuilder().makeCall(op, operands);
    }
    return ret;
  }

  private RexNode convertTimestampAddFunction(
      ResolvedFunctionCall functionCall,
      List<ResolvedColumn> columnList,
      List<RelDataTypeField> fieldList) {

    TimeUnit unit =
        TIME_UNIT_CASTING_MAP.get(
            ((ResolvedLiteral) functionCall.getArgumentList().get(2)).getValue().getEnumValue());

    if ((unit == TimeUnit.MICROSECOND) || (unit == TimeUnit.NANOSECOND)) {
      throw Status.UNIMPLEMENTED
          .withDescription("Micro and Nanoseconds are not supported by Beam ZetaSQL")
          .asRuntimeException();
    }

    SqlIntervalQualifier qualifier = new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);

    RexNode intervalArgumentNode =
        convertRexNodeFromResolvedExpr(
            functionCall.getArgumentList().get(1), columnList, fieldList);

    RexNode validatedIntervalArgument =
        rexBuilder()
            .makeCall(
                SqlOperators.VALIDATE_TIME_INTERVAL,
                intervalArgumentNode,
                rexBuilder().makeFlag(unit));

    RexNode intervalNode =
        rexBuilder()
            .makeCall(
                SqlStdOperatorTable.MULTIPLY,
                rexBuilder().makeIntervalLiteral(unit.multiplier, qualifier),
                validatedIntervalArgument);

    RexNode timestampNode =
        convertRexNodeFromResolvedExpr(
            functionCall.getArgumentList().get(0), columnList, fieldList);

    RexNode dateTimePlusResult =
        rexBuilder().makeCall(SqlStdOperatorTable.DATETIME_PLUS, timestampNode, intervalNode);

    RexNode validatedTimestampResult =
        rexBuilder().makeCall(SqlOperators.VALIDATE_TIMESTAMP, dateTimePlusResult);

    return validatedTimestampResult;
  }

  private RexNode convertIntervalToRexIntervalLiteral(ResolvedLiteral resolvedLiteral) {
    if (resolvedLiteral.getType().getKind() != TYPE_STRING) {
      throw new IllegalArgumentException(INTERVAL_FORMAT_MSG);
    }

    String valStr = resolvedLiteral.getValue().getStringValue();
    List<String> stringList =
        Arrays.stream(valStr.split(" ")).filter(s -> !s.isEmpty()).collect(Collectors.toList());

    if (stringList.size() != 3) {
      throw new IllegalArgumentException(INTERVAL_FORMAT_MSG);
    }

    if (!Ascii.toUpperCase(stringList.get(0)).equals("INTERVAL")) {
      throw new IllegalArgumentException(INTERVAL_FORMAT_MSG);
    }

    long intervalValue;
    try {
      intervalValue = Long.parseLong(stringList.get(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(INTERVAL_FORMAT_MSG, e);
    }

    String intervalDatepart = Ascii.toUpperCase(stringList.get(2));
    return createCalciteIntervalRexLiteral(intervalValue, intervalDatepart);
  }

  private RexLiteral createCalciteIntervalRexLiteral(long intervalValue, String intervalTimeUnit) {
    SqlIntervalQualifier sqlIntervalQualifier =
        convertIntervalDatepartToSqlIntervalQualifier(intervalTimeUnit);
    BigDecimal decimalValue;
    if (DATE_PART_UNITS_TO_MILLIS.contains(intervalTimeUnit)) {
      decimalValue = convertIntervalValueToMillis(sqlIntervalQualifier, intervalValue);
    } else if (DATE_PART_UNITS_TO_MONTHS.contains(intervalTimeUnit)) {
      decimalValue = new BigDecimal(intervalValue * 12);
    } else {
      decimalValue = new BigDecimal(intervalValue);
    }
    return rexBuilder().makeIntervalLiteral(decimalValue, sqlIntervalQualifier);
  }

  private static BigDecimal convertIntervalValueToMillis(
      SqlIntervalQualifier qualifier, long value) {
    switch (qualifier.typeName()) {
      case INTERVAL_DAY:
        return new BigDecimal(value * ONE_DAY_IN_MILLIS);
      case INTERVAL_HOUR:
        return new BigDecimal(value * ONE_HOUR_IN_MILLIS);
      case INTERVAL_MINUTE:
        return new BigDecimal(value * ONE_MINUTE_IN_MILLIS);
      case INTERVAL_SECOND:
        return new BigDecimal(value * ONE_SECOND_IN_MILLIS);
      default:
        throw new IllegalArgumentException(qualifier.typeName().toString());
    }
  }

  private static SqlIntervalQualifier convertIntervalDatepartToSqlIntervalQualifier(
      String datePart) {
    switch (datePart) {
      case "YEAR":
        return new SqlIntervalQualifier(TimeUnit.YEAR, null, SqlParserPos.ZERO);
      case "MONTH":
        return new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO);
      case "DAY":
        return new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
      case "HOUR":
        return new SqlIntervalQualifier(TimeUnit.HOUR, null, SqlParserPos.ZERO);
      case "MINUTE":
        return new SqlIntervalQualifier(TimeUnit.MINUTE, null, SqlParserPos.ZERO);
      case "SECOND":
        return new SqlIntervalQualifier(TimeUnit.SECOND, null, SqlParserPos.ZERO);
      case "WEEK":
        return new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO);
      case "QUARTER":
        return new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO);
      case "MILLISECOND":
        return new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, SqlParserPos.ZERO);
      default:
        throw new RuntimeException(
            String.format(
                "Received an undefined INTERVAL unit: %s. Please specify unit from the following"
                    + " list: %s.",
                datePart, INTERVAL_DATE_PART_MSG));
    }
  }

  private RexNode convertResolvedCast(
      ResolvedCast resolvedCast,
      List<ResolvedColumn> columnList,
      List<RelDataTypeField> fieldList) {
    TypeKind fromType = resolvedCast.getExpr().getType().getKind();
    TypeKind toType = resolvedCast.getType().getKind();
    isCastingSupported(fromType, toType);

    RexNode inputNode =
        convertRexNodeFromResolvedExpr(resolvedCast.getExpr(), columnList, fieldList);
    // nullability of the output type should match that of the input node's type
    RelDataType outputType =
        TypeUtils.toSimpleRelDataType(
            resolvedCast.getType().getKind(), rexBuilder(), inputNode.getType().isNullable());

    if (isZetaSQLCast(fromType, toType)) {
      return rexBuilder().makeCall(outputType, ZETASQL_CAST_OP, ImmutableList.of(inputNode));
    } else {
      return rexBuilder().makeCast(outputType, inputNode);
    }
  }

  private static void isCastingSupported(TypeKind fromType, TypeKind toType) {
    if (UNSUPPORTED_CASTING.containsKey(toType)
        && UNSUPPORTED_CASTING.get(toType).contains(fromType)) {
      throw new IllegalArgumentException(
          "Does not support CAST(" + fromType + " AS " + toType + ")");
    }
  }

  private static boolean isZetaSQLCast(TypeKind fromType, TypeKind toType) {
    // TODO: Structure ZETASQL_CAST_OP so that we don't have to repeat the supported types
    // here
    return (fromType.equals(TYPE_BYTES) && toType.equals(TYPE_STRING))
        || (fromType.equals(TYPE_INT64) && toType.equals(TYPE_BOOL))
        || (fromType.equals(TYPE_BOOL) && toType.equals(TYPE_INT64))
        || (fromType.equals(TYPE_TIMESTAMP) && toType.equals(TYPE_STRING));
  }

  private RexNode convertRexNodeFromResolvedColumnRefWithRefScan(
      ResolvedColumnRef columnRef,
      List<ResolvedColumn> refScanColumnList,
      List<ResolvedColumn> originalColumnList,
      List<RelDataTypeField> fieldList) {

    for (int i = 0; i < refScanColumnList.size(); i++) {
      if (refScanColumnList.get(i).getId() == columnRef.getColumn().getId()) {
        boolean nullable = fieldList.get(i).getType().isNullable();
        int off = (int) originalColumnList.get(i).getId() - 1;
        return rexBuilder()
            .makeInputRef(
                TypeUtils.toSimpleRelDataType(
                    columnRef.getType().getKind(), rexBuilder(), nullable),
                off);
      }
    }

    return null;
  }

  private RexNode convertResolvedParameter(ResolvedParameter parameter) {
    assert parameter.getType().equals(queryParams.get(parameter.getName()).getType());
    return convertValueToRexNode(
        queryParams.get(parameter.getName()).getType(), queryParams.get(parameter.getName()));
  }

  private RexNode convertResolvedStructFieldAccess(ResolvedGetStructField resolvedGetStructField) {
    return rexBuilder()
        .makeFieldAccess(
            convertRexNodeFromResolvedExpr(resolvedGetStructField.getExpr()),
            (int) resolvedGetStructField.getFieldIdx());
  }

  private RexNode convertResolvedStructFieldAccess(
      ResolvedGetStructField resolvedGetStructField,
      List<ResolvedColumn> columnList,
      List<RelDataTypeField> fieldList) {
    return rexBuilder()
        .makeFieldAccess(
            convertRexNodeFromResolvedExpr(resolvedGetStructField.getExpr(), columnList, fieldList),
            (int) resolvedGetStructField.getFieldIdx());
  }

  private RexBuilder rexBuilder() {
    return cluster.getRexBuilder();
  }

  private RelDataTypeFactory typeFactory() {
    return cluster.getTypeFactory();
  }
}
