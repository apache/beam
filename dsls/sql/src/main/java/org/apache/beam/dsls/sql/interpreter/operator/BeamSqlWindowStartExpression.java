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
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for {@code HOP_START}, {@code TUMBLE_START},
 * {@code SESSION_START} operation.
 *
 * <p>These operators returns the <em>start</em> timestamp of window.
 */
public class BeamSqlWindowStartExpression extends BeamSqlExpression {

  @Override
  public boolean accept() {
    return true;
  }

  @Override
  public BeamSqlPrimitive<Date> evaluate(BeamSqlRow inputRecord) {
    return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP,
        new Date(inputRecord.getWindowStart().getMillis()));
  }

}
