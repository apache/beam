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
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.operator.Operator;
import org.apache.beam.sdk.values.PCollection;

/**
 * A functor to translate an operator into a beam execution.
 *
 * @param <OperatorT> the type of the user defined euphoria operator definition
 */
@FunctionalInterface
interface OperatorTranslator<OperatorT extends Operator> {

  /**
   * Translates the given a operator it into a concrete beam transformation.
   *
   * @param operator the operator to translate
   * @param context the execution context aware of all inputs of the given operator
   * @return a beam transformation
   */
  PCollection<?> translate(OperatorT operator, BeamExecutorContext context);
}
