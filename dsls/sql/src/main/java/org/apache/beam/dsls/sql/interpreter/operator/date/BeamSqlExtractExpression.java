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

package org.apache.beam.dsls.sql.interpreter.operator.date;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for EXTRACT.
 */
public class BeamSqlExtractExpression extends BeamSqlExpression {
  public BeamSqlExtractExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.BIGINT);
  }
  @Override public boolean accept() {
    return operands.size() == 2
        && opType(1) == SqlTypeName.BIGINT;
  }

  @Override public BeamSqlPrimitive evaluate(BeamSqlRow inputRecord) {
    Long time = opValueEvaluated(1, inputRecord);
    Long extracted = DateTimeUtils.unixDateExtract((TimeUnitRange) op(0)
            .evaluate(inputRecord).getValue(),
        time);
    return BeamSqlPrimitive.of(SqlTypeName.BIGINT, extracted);
  }
}
