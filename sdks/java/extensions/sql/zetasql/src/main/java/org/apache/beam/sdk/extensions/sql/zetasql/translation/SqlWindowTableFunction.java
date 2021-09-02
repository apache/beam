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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.utils.TVFStreamingUtils;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelRecordType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlCallBinding;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlFunction;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.validate.SqlValidator;

/** Base class for table-valued function windowing operator (TUMBLE, HOP and SESSION). */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class SqlWindowTableFunction extends SqlFunction {
  public SqlWindowTableFunction(String name) {
    super(
        name,
        SqlKind.OTHER_FUNCTION,
        ARG0_TABLE_FUNCTION_WINDOWING,
        null,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(3);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    // There should only be three operands, and number of operands are checked before
    // this call.
    final SqlNode operand0 = callBinding.operand(0);
    final SqlValidator validator = callBinding.getValidator();
    final RelDataType type = validator.getValidatedNodeType(operand0);
    if (type.getSqlTypeName() != SqlTypeName.ROW) {
      return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
    }
    return true;
  }

  private boolean throwValidationSignatureErrorOrReturnFalse(
      SqlCallBinding callBinding, boolean throwOnFailure) {
    if (throwOnFailure) {
      throw callBinding.newValidationSignatureError();
    } else {
      return false;
    }
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return getName() + "(TABLE table_name, DESCRIPTOR(col1, col2 ...), datetime interval)";
  }

  /**
   * The first parameter of table-value function windowing is a TABLE parameter, which is not
   * scalar. So need to override SqlOperator.argumentMustBeScalar.
   */
  @Override
  public boolean argumentMustBeScalar(int ordinal) {
    return ordinal != 0;
  }

  /**
   * Type-inference strategy whereby the result type of a table function call is a ROW, which is
   * combined from the operand #0(TABLE parameter)'s schema and two additional fields:
   *
   * <ol>
   *   <li>window_start. TIMESTAMP type to indicate a window's start.
   *   <li>window_end. TIMESTAMP type to indicate a window's end.
   * </ol>
   */
  public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_WINDOWING =
      opBinding -> {
        RelDataType inputRowType = opBinding.getOperandType(0);
        List<RelDataTypeField> newFields = new ArrayList<>(inputRowType.getFieldList());
        RelDataType timestampType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

        RelDataTypeField windowStartField =
            new RelDataTypeFieldImpl(
                TVFStreamingUtils.WINDOW_START, newFields.size(), timestampType);
        newFields.add(windowStartField);
        RelDataTypeField windowEndField =
            new RelDataTypeFieldImpl(TVFStreamingUtils.WINDOW_END, newFields.size(), timestampType);
        newFields.add(windowEndField);

        return new RelRecordType(inputRowType.getStructKind(), newFields);
      };
}
