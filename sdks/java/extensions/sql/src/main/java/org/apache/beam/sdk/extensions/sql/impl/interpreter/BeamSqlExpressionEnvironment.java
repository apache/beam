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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.values.Row;

/**
 * Environment in which a {@link BeamSqlExpression} is evaluated. This includes bindings of
 * correlation variables and local references.
 */
public interface BeamSqlExpressionEnvironment {

  /** Gets the value for a local variable reference. */
  BeamSqlPrimitive<?> getLocalRef(int localRefIndex);

  /** Gets the value for a correlation variable. */
  Row getCorrelVariable(int correlVariableId);

  /**
   * An environment that shares input row, window, and correlation variables but local refs are
   * replaced with the given unevaluated expressions.
   */
  BeamSqlExpressionEnvironment copyWithLocalRefExprs(List<BeamSqlExpression> localRefExprs);
}
