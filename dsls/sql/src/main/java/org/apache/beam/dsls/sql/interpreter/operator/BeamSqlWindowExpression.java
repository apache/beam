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

import java.util.Date;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for {@code HOP}, {@code TUMBLE}, {@code SESSION} operation.
 *
 * <p>These functions don't change the timestamp field, instead it's used to indicate
 * the event_timestamp field, and how the window is defined.
 */
public class BeamSqlWindowExpression extends BeamSqlExpression {

  public BeamSqlWindowExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  @Override
  public boolean accept() {
    return operands.get(0).getOutputType().equals(SqlTypeName.DATE)
        || operands.get(0).getOutputType().equals(SqlTypeName.TIME)
        || operands.get(0).getOutputType().equals(SqlTypeName.TIMESTAMP);
  }

  @Override
  public BeamSqlPrimitive<Date> evaluate(BeamSQLRow inputRecord) {
    return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP,
        (Date) operands.get(0).evaluate(inputRecord).getValue());
  }

}
