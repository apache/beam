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

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** UDF to expose {@link PaneInfo}. */
public class BeamPaneInfoUdf extends BeamSqlExpression {
  /** supported pane functions. */
  public enum PaneFunc {
    FIRST_PANE,
    LAST_PANE,
    PANE_TIMING,
    PANE_INDEX;
  }

  private PaneFunc func;

  public BeamPaneInfoUdf(PaneFunc func) {
    this.func = func;
  }

  @Override
  public boolean accept() {
    return true;
  }

  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, PaneInfo paneInfo, BeamSqlExpressionEnvironment env) {
    switch (func) {
      case FIRST_PANE:
        return BeamSqlPrimitive.<Integer>of(SqlTypeName.INTEGER, paneInfo.isFirst() ? 1 : 0);
      case LAST_PANE:
        return BeamSqlPrimitive.<Integer>of(SqlTypeName.INTEGER, paneInfo.isLast() ? 1 : 0);
      case PANE_INDEX:
        return BeamSqlPrimitive.<Long>of(SqlTypeName.BIGINT, paneInfo.getIndex());
      case PANE_TIMING:
        return BeamSqlPrimitive.<String>of(SqlTypeName.VARCHAR, paneInfo.getTiming().name());
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    throw new IllegalStateException();
  }

  /** UDF instance for FIRST_PANE. */
  public static class BeamUdfIsFirstPane extends BeamPaneInfoUdf implements BeamSqlUdf {

    public BeamUdfIsFirstPane(PaneFunc func) {
      super(func);
    }

    public static Integer eval() {
      return -1;
    }
  }

  /** UDF instance for LAST_PANE. */
  public static class BeamUdfIsLastPane extends BeamPaneInfoUdf implements BeamSqlUdf {

    public BeamUdfIsLastPane(PaneFunc func) {
      super(func);
    }

    public static Integer eval() {
      return -1;
    }
  }

  /** UDF instance for PANE_INDEX. */
  public static class BeamUdfPaneIndex extends BeamPaneInfoUdf implements BeamSqlUdf {

    public BeamUdfPaneIndex(PaneFunc func) {
      super(func);
    }

    public static Long eval() {
      return -1L;
    }
  }

  /** UDF instance for PANE_TIMING. */
  public static class BeamUdfPaneTiming extends BeamPaneInfoUdf implements BeamSqlUdf {

    public BeamUdfPaneTiming(PaneFunc func) {
      super(func);
    }

    public static String eval() {
      return PaneInfo.Timing.UNKNOWN.name();
    }
  }
}
