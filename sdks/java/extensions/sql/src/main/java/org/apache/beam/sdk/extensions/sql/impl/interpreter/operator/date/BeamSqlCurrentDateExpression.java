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

import java.util.Collections;
import java.util.Date;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for CURRENT_DATE and LOCALTIME.
 *
 * <p>Returns the current date in the session time zone, in a value of datatype DATE.
 */
public class BeamSqlCurrentDateExpression extends BeamSqlExpression {
  public BeamSqlCurrentDateExpression() {
    super(Collections.emptyList(), SqlTypeName.DATE);
  }

  @Override public boolean accept() {
    return getOperands().size() == 0;
  }

  @Override public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    return BeamSqlPrimitive.of(outputType, new Date());
  }
}
