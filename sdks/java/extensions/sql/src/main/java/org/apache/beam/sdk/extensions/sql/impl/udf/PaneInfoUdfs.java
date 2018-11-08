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
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * UDFs to expose {@link org.apache.beam.sdk.transforms.windowing.PaneInfo} related on {@link Row}.
 */
@AutoService(BeamBuiltinFunctionProvider.class)
public class PaneInfoUdfs extends BeamBuiltinFunctionProvider {
  @UDF(
    funcName = "FIRST_PANE",
    parameterArray = {},
    returnType = Schema.TypeName.BOOLEAN
  )
  public Boolean isFirstPane() {
    return false;
  }

  @UDF(
    funcName = "LAST_PANE",
    parameterArray = {},
    returnType = Schema.TypeName.BOOLEAN
  )
  public Boolean isLastPane() {
    return false;
  }

  @UDF(
    funcName = "PANE_TIMING",
    parameterArray = {},
    returnType = Schema.TypeName.STRING
  )
  public String paneTiming() {
    return "UNKNOWN";
  }

  @UDF(
    funcName = "PANE_INDEX",
    parameterArray = {},
    returnType = Schema.TypeName.INT64
  )
  public Long paneIndex() {
    return 0L;
  }

  /** UDF returns if it's the first pane. */
  public static class IsFirstPane extends BeamSqlExpression {
    public IsFirstPane() {
      super(Collections.emptyList(), SqlTypeName.BOOLEAN);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, env.getPaneInfo().isFirst());
    }
  }

  /** UDF returns if it's the last pane. */
  public static class IsLastPane extends BeamSqlExpression {
    public IsLastPane() {
      super(Collections.emptyList(), SqlTypeName.BOOLEAN);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(getOutputType(), env.getPaneInfo().isLast());
    }
  }

  /** UDF returns index of the pane, start from 0. */
  public static class PaneIndex extends BeamSqlExpression {
    public PaneIndex() {
      super(Collections.emptyList(), SqlTypeName.BIGINT);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(getOutputType(), env.getPaneInfo().getIndex());
    }
  }

  /** UDF returns {@link org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing} for this pane. */
  public static class PaneTiming extends BeamSqlExpression {
    public PaneTiming() {
      super(Collections.emptyList(), SqlTypeName.VARCHAR);
    }

    @Override
    public boolean accept() {
      return true;
    }

    @Override
    public BeamSqlPrimitive evaluate(Row inputRow, BeamSqlExpressionEnvironment env) {
      return BeamSqlPrimitive.of(getOutputType(), env.getPaneInfo().getTiming().name());
    }
  }
}
