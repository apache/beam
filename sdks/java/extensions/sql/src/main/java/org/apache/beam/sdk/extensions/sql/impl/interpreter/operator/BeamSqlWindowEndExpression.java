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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;

/**
 * {@code BeamSqlExpression} for {@code HOP_END}, {@code TUMBLE_END}, {@code SESSION_END} operation.
 *
 * <p>These operators returns the <em>end</em> timestamp of window.
 */
public class BeamSqlWindowEndExpression extends BeamSqlExpression {

  @Override
  public boolean accept() {
    return true;
  }

  @Override
  public BeamSqlPrimitive<DateTime> evaluate(Row inputRow, BoundedWindow window) {
    if (window instanceof IntervalWindow) {
      return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP,
          new DateTime(((IntervalWindow) window).end()));
    } else {
      throw new UnsupportedOperationException(
          "Cannot run HOP_END|TUMBLE_END|SESSION_END on GlobalWindow.");
    }
  }

}
