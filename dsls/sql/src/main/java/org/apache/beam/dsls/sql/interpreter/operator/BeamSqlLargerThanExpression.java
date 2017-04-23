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
import org.apache.beam.dsls.sql.exception.BeamInvalidOperatorException;

/**
 * {@code BeamSqlExpression} for {@code >} operation.
 */
public class BeamSqlLargerThanExpression extends BeamSqlCompareExpression {

  public BeamSqlLargerThanExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override
  public Boolean compare(CharSequence leftValue, CharSequence rightValue) {
    return String.valueOf(leftValue).compareTo(String.valueOf(rightValue)) > 0;
  }

  @Override
  public Boolean compare(Boolean leftValue, Boolean rightValue) {
    throw new BeamInvalidOperatorException("> is not supported for Boolean.");
  }

  @Override
  public Boolean compare(Number leftValue, Number rightValue) {
    return (leftValue == null && rightValue == null)
        || (leftValue != null && rightValue != null
              && leftValue.floatValue() > (rightValue).floatValue());
  }

}
