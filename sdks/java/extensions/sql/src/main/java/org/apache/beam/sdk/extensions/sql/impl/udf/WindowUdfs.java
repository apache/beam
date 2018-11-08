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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import com.google.auto.service.AutoService;
import java.util.Collections;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.Instant;

/** /** UDFs to expose {@link } related on {@link org.apache.beam.sdk.values.Row}. */
@AutoService(BeamBuiltinFunctionProvider.class)
public class WindowUdfs extends BeamBuiltinFunctionProvider {
  @UDF(
    funcName = "WINDOW_TYPE",
    parameterArray = {},
    returnType = Schema.TypeName.STRING
  )
  public String windowType() {
    return "UNKNOWN";
  }

  @UDF(
    funcName = "WINDOW_START",
    parameterArray = {},
    returnType = Schema.TypeName.DATETIME
  )
  public Instant windowStartTime() {
    return Instant.now();
  }

  @UDF(
    funcName = "WINDOW_END",
    parameterArray = {},
    returnType = Schema.TypeName.DATETIME
  )
  public Instant windowEndTime() {
    return Instant.now();
  }

  /** UDF returns 'GLOBAL' for {@link GlobalWindow}, and 'INTERVAL' for others. */
  public static class WindowType extends BeamSqlExpression {
    public WindowType() {
      super(Collections.emptyList(), SqlTypeName.VARCHAR);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(
          getOutputType(), env.getWindow() instanceof GlobalWindow ? "GLOBAL" : "INTERVAL");
    }
  }

  /** UDF returns start {@link Instant} for window, inclusive. */
  public static class WindowStart extends BeamSqlExpression {
    public WindowStart() {
      super(Collections.emptyList(), SqlTypeName.TIMESTAMP);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(
          getOutputType(),
          env.getWindow() instanceof GlobalWindow
              ? GlobalWindow.TIMESTAMP_MIN_VALUE
              : ((IntervalWindow) env.getWindow()).start());
    }
  }

  /** UDF returns end {@link Instant} for window, exclusive. */
  public static class WindowEnd extends BeamSqlExpression {
    public WindowEnd() {
      super(Collections.emptyList(), SqlTypeName.TIMESTAMP);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(
          getOutputType(),
          env.getWindow() instanceof GlobalWindow
              ? GlobalWindow.TIMESTAMP_MAX_VALUE
              : ((IntervalWindow) env.getWindow()).end());
    }
  }
}
