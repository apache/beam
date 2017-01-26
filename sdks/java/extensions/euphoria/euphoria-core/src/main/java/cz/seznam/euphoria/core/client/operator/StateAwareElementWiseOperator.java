/**
 * Copyright 2016 Seznam a.s.
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


package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator working with some state element-wise.
 */
public class StateAwareElementWiseOperator<IN, KEY, OUT>
    extends ElementWiseOperator<IN, OUT> implements StateAware<IN, KEY> {

  private final UnaryFunction<IN, KEY> keyExtractor;
  private final Partitioning<KEY> partitioning;

  protected StateAwareElementWiseOperator(
          String name, Flow flow, Dataset<IN> input,
          UnaryFunction<IN, KEY> keyExtractor,
          Partitioning<KEY> partitioning) {

    super(name, flow, input);
    this.keyExtractor = keyExtractor;
    this.partitioning = partitioning;
  }

  @Override
  public UnaryFunction<IN, KEY> getKeyExtractor() {
    return keyExtractor;
  }

  @Override
  public Partitioning<KEY> getPartitioning() {
    return partitioning;
  }

}
