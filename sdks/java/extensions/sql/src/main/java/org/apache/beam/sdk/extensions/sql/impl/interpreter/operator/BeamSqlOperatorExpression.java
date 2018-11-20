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
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;

/** A generic expression form for an operator applied to arguments. */
public class BeamSqlOperatorExpression extends BeamSqlExpression {

  private final BeamSqlOperator operator;

  public BeamSqlOperatorExpression(BeamSqlOperator operator, List<BeamSqlExpression> operands) {
    super(operands, operator.getOutputType());
    this.operator = operator;
  }

  @Override
  public boolean accept() {
    return operator.accept(operands);
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    List<BeamSqlPrimitive> arguments =
        operands
            .stream()
            .map(operand -> operand.evaluate(inputRow, window, env))
            .collect(Collectors.toList());

    return operator.apply(arguments);
  }
}
