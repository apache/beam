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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Infix '-' operation for timestamps.
 *
 * <p>Implements 2 SQL subtraction operations at the moment: 'timestampdiff(timeunit, timestamp,
 * timestamp)', and 'timestamp - interval'
 *
 * <p>Calcite converts both of the above into infix '-' expression, with different operands and
 * return types.
 *
 * <p>This class delegates evaluation to specific implementation of one of the above operations, see
 * {@link BeamSqlTimestampMinusTimestampExpression} and {@link
 * BeamSqlTimestampMinusIntervalExpression}
 *
 * <p>Calcite supports one more subtraction kind: 'interval - interval', but it is not implemented
 * yet.
 */
public class BeamSqlDatetimeMinusExpression extends BeamSqlExpression {
  private BeamSqlExpression delegateExpression;

  public BeamSqlDatetimeMinusExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);

    this.delegateExpression = createDelegateExpression(operands, outputType);
  }

  private BeamSqlExpression createDelegateExpression(
      List<BeamSqlExpression> operands, SqlTypeName outputType) {
    if (isTimestampMinusTimestamp(operands, outputType)) {
      return new BeamSqlTimestampMinusTimestampExpression(operands, outputType);
    } else if (isTimestampMinusInterval(operands, outputType)) {
      return new BeamSqlTimestampMinusIntervalExpression(operands, outputType);
    } else if (isDatetimeMinusInterval(operands, outputType)) {
      return new BeamSqlDatetimeMinusIntervalExpression(operands, outputType);
    }

    return null;
  }

  private boolean isTimestampMinusTimestamp(
      List<BeamSqlExpression> operands, SqlTypeName outputType) {

    return BeamSqlTimestampMinusTimestampExpression.accept(operands, outputType);
  }

  private boolean isTimestampMinusInterval(
      List<BeamSqlExpression> operands, SqlTypeName outputType) {

    return BeamSqlTimestampMinusIntervalExpression.accept(operands, outputType);
  }

  private boolean isDatetimeMinusInterval(
      List<BeamSqlExpression> operands, SqlTypeName outputType) {

    return BeamSqlDatetimeMinusIntervalExpression.accept(operands, outputType);
  }

  @Override
  public boolean accept() {
    return delegateExpression != null && delegateExpression.accept();
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    if (delegateExpression == null) {
      throw new IllegalStateException("Unable to execute unsupported 'datetime minus' expression");
    }

    return delegateExpression.evaluate(inputRow, window, env);
  }
}
