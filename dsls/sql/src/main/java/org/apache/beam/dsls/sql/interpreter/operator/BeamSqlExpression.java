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

import java.io.Serializable;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} is an equivalent expression in BeamSQL, of {@link RexNode} in Calcite.
 *
 * <p>An implementation of {@link BeamSqlExpression} takes one or more {@code BeamSqlExpression}
 * as its operands, and return a value with type {@link SqlTypeName}.
 *
 */
public abstract class BeamSqlExpression implements Serializable{
  protected List<BeamSqlExpression> operands;
  protected SqlTypeName outputType;

  protected BeamSqlExpression(){}

  public BeamSqlExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    this.operands = operands;
    this.outputType = outputType;
  }

  /**
   * assertion to make sure the input and output are supported in this expression.
   */
  public abstract boolean accept();

  /**
   * Apply input record {@link BeamSQLRow} to this expression,
   * the output value is wrapped with {@link BeamSqlPrimitive}.
   */
  public abstract BeamSqlPrimitive evaluate(BeamSQLRow inputRecord);

  public List<BeamSqlExpression> getOperands() {
    return operands;
  }

  public SqlTypeName getOutputType() {
    return outputType;
  }
}
