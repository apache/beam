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
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Arrays;
import java.util.Collection;

/**
 * Operator with single input.
 */
public abstract class SingleInputOperator<IN, OUT> extends Operator<IN, OUT> {

  final Dataset<IN> input;

  protected SingleInputOperator(String name, Flow flow, Dataset<IN> input) {
    super(name, flow);
    this.input = input;
  }

  /** Retrieve input of this operator. */
  public Dataset<IN> input() {
    return input;
  }

  /** List all inputs (single input). */
  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(input);
  }



}
