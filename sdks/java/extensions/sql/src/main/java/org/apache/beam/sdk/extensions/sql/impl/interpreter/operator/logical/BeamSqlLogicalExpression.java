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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.logical;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for Logical operators.
 */
public abstract class BeamSqlLogicalExpression extends BeamSqlExpression {
  private BeamSqlLogicalExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  public BeamSqlLogicalExpression(List<BeamSqlExpression> operands) {
    this(operands, SqlTypeName.BOOLEAN);
  }

  @Override
  public boolean accept() {
    for (BeamSqlExpression exp : operands) {
      // only accept BOOLEAN expression as operand
      if (!exp.getOutputType().equals(SqlTypeName.BOOLEAN)) {
        return false;
      }
    }
    return true;
  }
}
