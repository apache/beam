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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

/** An operator that is applied to already-evaluated arguments. */
public interface BeamSqlUnaryOperator extends BeamSqlOperator {

  @Override
  default BeamSqlPrimitive apply(List<BeamSqlPrimitive> arguments) {
    checkArgument(arguments.size() == 1, "Unary operator %s received more than one argument", this);
    return apply(arguments.get(0));
  }

  @Override
  default boolean accept(List<BeamSqlExpression> arguments) {
    return arguments.size() == 1 && accept(arguments.get(0));
  }

  boolean accept(BeamSqlExpression argument);

  BeamSqlPrimitive apply(BeamSqlPrimitive argument);
}
