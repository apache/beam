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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Implements DOT operator to access fields of dynamic ROWs.
 */
public class BeamSqlDotExpression extends BeamSqlExpression {

  public BeamSqlDotExpression(List<BeamSqlExpression> operands, SqlTypeName sqlTypeName) {
    super(operands, sqlTypeName);
  }

  @Override
  public boolean accept() {
    return
        operands.size() == 2
        && SqlTypeName.ROW.equals(operands.get(0).getOutputType())
        && (SqlTypeName.VARCHAR.equals(operands.get(1).getOutputType())
            || SqlTypeName.CHAR.equals(operands.get(1).getOutputType()));
  }

  @Override
  public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    Row dynamicRow = opValueEvaluated(0, inputRow, window);
    String fieldName = opValueEvaluated(1, inputRow, window);
    SqlTypeName fieldType = getFieldType(dynamicRow, fieldName);

    return BeamSqlPrimitive.of(fieldType, dynamicRow.getValue(fieldName));
  }

  private SqlTypeName getFieldType(Row row, String fieldName) {
    Schema.Field field = row.getSchema().getField(fieldName);
    return CalciteUtils.toSqlTypeName(field.getType());
  }
}
