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
package org.apache.beam.dsls.sql.interpreter.operator;

import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for 'AND' operation.
 */
public class BeamSqlAndExpression extends BeamSqlExpression {

  private BeamSqlAndExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }
  public BeamSqlAndExpression(List<BeamSqlExpression> operands) {
    this(operands, SqlTypeName.BOOLEAN);
  }

  @Override
  public boolean accept() {
    for (BeamSqlExpression exp : operands) {
      // only accept BOOLEAN expression as operand
      if (!exp.outputType.equals(SqlTypeName.BOOLEAN)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public BeamSqlPrimitive<Boolean> evaluate(BeamSQLRow inputRecord) {
    boolean result = true;
    for (BeamSqlExpression exp : operands) {
      BeamSqlPrimitive<Boolean> expOut = exp.evaluate(inputRecord);
      result = result && expOut.getValue();
      if (!result) {
        break;
      }
    }
    return BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, result);
  }

}
