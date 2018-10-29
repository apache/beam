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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.udf;

import java.util.Date;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** UDF to expose {@link BoundedWindow}. */
public class BeamBoundedWindowUdf extends BeamSqlExpression {
  /** supported window functions. */
  public enum BoundedWindowFunc {
    WINDOW_TYPE,
    WINDOW_START,
    WINDOW_END;
  }

  private BoundedWindowFunc func;

  public BeamBoundedWindowUdf(BoundedWindowFunc func) {
    this.func = func;
  }

  @Override
  public boolean accept() {
    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, PaneInfo paneInfo, BeamSqlExpressionEnvironment env) {
    switch (func) {
      case WINDOW_TYPE:
        return BeamSqlPrimitive.<String>of(
            SqlTypeName.VARCHAR, window instanceof GlobalWindow ? "GLOBAL" : "INTERVAL");
      case WINDOW_START:
        return BeamSqlPrimitive.<Date>of(
            SqlTypeName.TIMESTAMP,
            window instanceof GlobalWindow
                ? window.TIMESTAMP_MIN_VALUE.toDate()
                : ((IntervalWindow) window).start().toDate());
      case WINDOW_END:
        return BeamSqlPrimitive.<Date>of(
            SqlTypeName.TIMESTAMP,
            window instanceof GlobalWindow
                ? window.TIMESTAMP_MAX_VALUE.toDate()
                : ((IntervalWindow) window).end().toDate());
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    throw new IllegalStateException();
  }

  /** UDF instance for WINDOW_TYPE. */
  public static class BeamUdfWindowType extends BeamBoundedWindowUdf implements BeamSqlUdf {

    public BeamUdfWindowType(BoundedWindowFunc func) {
      super(func);
    }

    public static String eval() {
      return "UNKNOWN";
    }
  }

  /** UDF instance for WINDOW_START. */
  public static class BeamUdfWindowStart extends BeamBoundedWindowUdf implements BeamSqlUdf {

    public BeamUdfWindowStart(BoundedWindowFunc func) {
      super(func);
    }

    public static Date eval() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE.toDate();
    }
  }

  /** UDF instance for WINDOW_END. */
  public static class BeamUdfWindowEnd extends BeamBoundedWindowUdf implements BeamSqlUdf {

    public BeamUdfWindowEnd(BoundedWindowFunc func) {
      super(func);
    }

    public static Date eval() {
      return BoundedWindow.TIMESTAMP_MAX_VALUE.toDate();
    }
  }
}
