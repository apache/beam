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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.comparison;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@link BeamSqlCompareExpression} is used for compare operations.
 *
 * <p>See {@link BeamSqlEqualsExpression}, {@link BeamSqlLessThanExpression},
 * {@link BeamSqlLessThanOrEqualsExpression}, {@link BeamSqlGreaterThanExpression},
 * {@link BeamSqlGreaterThanOrEqualsExpression} and {@link BeamSqlNotEqualsExpression}
 * for more details.
 *
 */
public abstract class BeamSqlCompareExpression extends BeamSqlExpression {

  private BeamSqlCompareExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  public BeamSqlCompareExpression(List<BeamSqlExpression> operands) {
    this(operands, SqlTypeName.BOOLEAN);
  }

  /**
   * Compare operation must have 2 operands.
   */
  @Override
  public boolean accept() {
    return operands.size() == 2;
  }

  @Override
  public BeamSqlPrimitive<Boolean> evaluate(Row inputRow, BoundedWindow window) {
    Object leftValue = operands.get(0).evaluate(inputRow, window).getValue();
    Object rightValue = operands.get(1).evaluate(inputRow, window).getValue();
    switch (operands.get(0).getOutputType()) {
    case BIGINT:
    case DECIMAL:
    case DOUBLE:
    case FLOAT:
    case INTEGER:
    case SMALLINT:
    case TINYINT:
      return BeamSqlPrimitive.of(SqlTypeName.BOOLEAN,
          compare((Number) leftValue, (Number) rightValue));
    case BOOLEAN:
      return BeamSqlPrimitive.of(SqlTypeName.BOOLEAN,
          compare((Boolean) leftValue, (Boolean) rightValue));
    case VARCHAR:
      return BeamSqlPrimitive.of(SqlTypeName.BOOLEAN,
          compare((CharSequence) leftValue, (CharSequence) rightValue));
    default:
      throw new UnsupportedOperationException(toString());
    }
  }

  /**
   * Compare between String values, mapping to {@link SqlTypeName#VARCHAR}.
   */
  public abstract Boolean compare(CharSequence leftValue, CharSequence rightValue);

  /**
   * Compare between Boolean values, mapping to {@link SqlTypeName#BOOLEAN}.
   */
  public abstract Boolean compare(Boolean leftValue, Boolean rightValue);

  /**
   * Compare between Number values, including {@link SqlTypeName#BIGINT},
   * {@link SqlTypeName#DECIMAL}, {@link SqlTypeName#DOUBLE}, {@link SqlTypeName#FLOAT},
   * {@link SqlTypeName#INTEGER}, {@link SqlTypeName#SMALLINT} and {@link SqlTypeName#TINYINT}.
   */
  public abstract Boolean compare(Number leftValue, Number rightValue);


}
