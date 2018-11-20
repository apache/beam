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

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} is an equivalent expression in BeamSQL, of {@link RexNode} in Calcite.
 *
 * <p>An implementation of {@link BeamSqlExpression} takes one or more {@code BeamSqlExpression} as
 * its operands, and return a value with type {@link SqlTypeName}.
 */
public abstract class BeamSqlExpression implements Serializable {
  protected List<BeamSqlExpression> operands;
  protected SqlTypeName outputType;

  protected BeamSqlExpression() {}

  public BeamSqlExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    this.operands = operands;
    this.outputType = outputType;
  }

  public BeamSqlExpression op(int idx) {
    return operands.get(idx);
  }

  public SqlTypeName opType(int idx) {
    return op(idx).getOutputType();
  }

  public Object opValueEvaluated(
      int idx, Row row, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    return op(idx).evaluate(row, window, env).getValue();
  }

  /** assertion to make sure the input and output are supported in this expression. */
  public abstract boolean accept();

  /**
   * Apply input record {@link Row} with {@link BoundedWindow} to this expression, the output value
   * is wrapped with {@link BeamSqlPrimitive}.
   */
  public abstract BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env);

  public List<BeamSqlExpression> getOperands() {
    return operands;
  }

  public SqlTypeName getOutputType() {
    return outputType;
  }

  public int numberOfOperands() {
    return operands.size();
  }
}
