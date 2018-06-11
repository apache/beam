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
package org.apache.beam.sdk.extensions.sql.impl.interpreter;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;

/**
 * Implementations of {@link BeamSqlExpressionEnvironment}.
 *
 * <p>Use of arrays is efficient and safe, as Calcite generates variables densely packed and
 * referenced by index.
 */
public class BeamSqlExpressionEnvironments {

  /**
   * An empty environment, for contexts where it is certain there will be no ref access.
   *
   * <p>Since there are no expressions, there need not be a row or window.
   *
   * <p>This environment cannot be extended with local expressions, and will throw {@link
   * UnsupportedOperationException}.
   */
  public static BeamSqlExpressionEnvironment empty() {
    return new EmptyEnvironment();
  }

  /** An environment with a fixed row and window but not expressions or correlation variables. */
  public static BeamSqlExpressionEnvironment forRow(Row row, BoundedWindow window) {
    return new ListEnvironment(row, window, new ArrayList<>(), new ArrayList<>());
  }

  /** An environment with a fixed row and window and correlation variables. */
  public static BeamSqlExpressionEnvironment forRowAndCorrelVariables(
      Row row, BoundedWindow window, List<Row> correlVariables) {
    return new ListEnvironment(row, window, correlVariables, new ArrayList<>());
  }

  /** Basic environment implementation. */
  private static class ListEnvironment implements BeamSqlExpressionEnvironment {

    private final Row inputRow;
    private final BoundedWindow window;

    /** Local expressions; once evaluated they are replaced with the evaluated value in place. */
    private final List<BeamSqlExpression> localRefExprs;

    private final List<Row> correlVariables;

    public ListEnvironment(
        Row inputRow,
        BoundedWindow window,
        List<Row> correlVariables,
        List<BeamSqlExpression> localRefExprs) {
      this.inputRow = inputRow;
      this.window = window;
      this.correlVariables = correlVariables;
      this.localRefExprs = new ArrayList<>(localRefExprs);
    }

    @Override
    public BeamSqlPrimitive<?> getLocalRef(int localRefIndex) {
      checkArgument(
          localRefIndex < localRefExprs.size(),
          "Local ref %s not found in %s",
          localRefIndex,
          this);

      BeamSqlExpression expr = localRefExprs.get(localRefIndex);
      BeamSqlPrimitive<?> value = expr.evaluate(inputRow, window, this);
      if (!(expr instanceof BeamSqlPrimitive)) {
        localRefExprs.set(localRefIndex, value);
      }
      return value;
    }

    @Override
    public Row getCorrelVariable(int correlVariableId) {
      checkArgument(
          correlVariableId < correlVariables.size(),
          "Correlation variable %s not found in %s",
          correlVariableId,
          this);

      return correlVariables.get(correlVariableId);
    }

    @Override
    public BeamSqlExpressionEnvironment copyWithLocalRefExprs(
        List<BeamSqlExpression> localRefExprs) {
      return new ListEnvironment(inputRow, window, correlVariables, localRefExprs);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("inputRow", inputRow)
          .add("window", window)
          .add("correlVariables", correlVariables)
          .add("localRefExprs", localRefExprs)
          .toString();
    }
  }

  private static class EmptyEnvironment implements BeamSqlExpressionEnvironment {
    @Override
    public BeamSqlPrimitive<?> getLocalRef(int localRefIndex) {
      throw new UnsupportedOperationException(
          String.format("%s does not support local references", getClass().getSimpleName()));
    }

    @Override
    public Row getCorrelVariable(int correlVariableId) {
      throw new UnsupportedOperationException(
          String.format("%s does not support correlation variables", getClass().getSimpleName()));
    }

    @Override
    public BeamSqlExpressionEnvironment copyWithLocalRefExprs(
        List<BeamSqlExpression> localRefExprs) {
      throw new UnsupportedOperationException(
          String.format("%s does not support local references", getClass().getSimpleName()));
    }
  }
}
