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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

/** Operator operating on window level with state information. */
@Audience(Audience.Type.INTERNAL)
public class StateAwareWindowWiseSingleInputOperator<
        IN,
        WIN,
        KIN,
        KEY,
        OUT,
        W extends Window<W>,
        OP extends StateAwareWindowWiseSingleInputOperator<IN, WIN, KIN, KEY, OUT, W, OP>>
    extends StateAwareWindowWiseOperator<IN, WIN, KIN, KEY, OUT, W, OP> {

  protected final Dataset<IN> input;
  private final Dataset<OUT> output;

  protected StateAwareWindowWiseSingleInputOperator(
      String name,
      Flow flow,
      Dataset<IN> input,
      UnaryFunction<KIN, KEY> extractor,
      @Nullable Windowing<WIN, W> windowing,
      Set<OutputHint> outputHints) {

    super(name, flow, windowing, extractor);
    this.input = input;
    this.output = createOutput(input, outputHints);
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
