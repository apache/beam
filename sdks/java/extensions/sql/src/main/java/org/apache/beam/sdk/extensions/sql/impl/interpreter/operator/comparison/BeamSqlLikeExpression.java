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
import org.apache.calcite.runtime.SqlFunctions;
import org.joda.time.DateTime;

/** {@code BeamSqlExpression} for 'LIKE' operation. */
public class BeamSqlLikeExpression extends BeamSqlCompareExpression {

  public BeamSqlLikeExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override
  public Boolean compare(CharSequence leftValue, CharSequence rightValue) {
    return SqlFunctions.like(leftValue.toString(), rightValue.toString());
  }

  @Override
  public Boolean compare(Boolean leftValue, Boolean rightValue) {
    throw new IllegalArgumentException("LIKE is not supported for Boolean.");
  }

  @Override
  public Boolean compare(Number leftValue, Number rightValue) {
    throw new IllegalArgumentException("LIKE is not supported for Number.");
  }

  @Override
  public Boolean compare(DateTime leftValue, DateTime rightValue) {
    throw new IllegalArgumentException("LIKE is not supported for DateTime.");
  }
}
