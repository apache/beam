/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * A functor to translate an operator into a flink execution.
 *
 * @param <O> the type of the user defined euphoria operator definition
 * @param <F> the type of the euphoria-flink representation of user defined operators
 * @param <E> the type of flink's execution environment object
 * @param <D> the type of the translation result; typically either flink's a data-set or data-stream
 * @param <C> the type of the execution context
 */
public interface OperatorTranslator<
    O extends Operator,
    F extends FlinkOperator<O>,
    E,
    D,
    C extends ExecutorContext<E, D>> {

  /**
   * Translates the given a operator it into a concrete data-set/data-stream flink
   * transformation.
   *
   * @param operator the operator to translate (an euphoria-flink representation of the
   *         user's original operator definition)
   * @param context the execution context aware of all inputs of the given operator
   *
   * @return a data-set or data-stream (depending on the execution context)
   *          representing the transformations of the given operator on flink's
   *          execution engine; never {@code null}
   */
  D translate(F operator, C context);

}
