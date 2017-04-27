/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;

/**
 * Operator operating on window level with state information.
 */
public class StateAwareWindowWiseSingleInputOperator<
    IN, WIN, KIN, KEY, OUT, W extends Window,
    OP extends StateAwareWindowWiseSingleInputOperator<IN, WIN, KIN, KEY, OUT, W, OP>>
    extends StateAwareWindowWiseOperator<IN, WIN, KIN, KEY, OUT, W, OP> {

  protected final Dataset<IN> input;
  private final Dataset<OUT> output;

  protected StateAwareWindowWiseSingleInputOperator(
          String name,
          Flow flow, Dataset<IN> input,
          UnaryFunction<KIN, KEY> extractor,
          @Nullable Windowing<WIN, W> windowing,
          @Nullable ExtractEventTime<WIN> eventTimeAssigner,
          Partitioning<KEY> partitioning) {
    
    super(name, flow, windowing, eventTimeAssigner, extractor, partitioning);
    this.input = input;
    this.output = createOutput(input);
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Collections.singletonList(input);
  }

  public Dataset<IN> input() {
    return input;
  }

  @Override
  public Dataset<OUT> output() {
    return output;
  }
}
