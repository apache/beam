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

import cz.seznam.euphoria.core.annotation.operator.Basic;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;

import java.util.Objects;

/**
 * Flat map operator on dataset.
 */
@Basic(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class FlatMap<IN, OUT> extends ElementWiseOperator<IN, OUT> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> UsingBuilder<IN> of(Dataset<IN> input) {
      return new UsingBuilder<>(name, input);
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunctor<IN, OUT> functor) {
      return new OutputBuilder<>(name, input, functor);
    }
  }

  public static class OutputBuilder<IN, OUT>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<OUT>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunctor<IN, OUT> functor;

    OutputBuilder(String name, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
      this.name = name;
      this.input = input;
      this.functor = functor;
    }

    @Override
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      FlatMap<IN, OUT> map = new FlatMap<>(name, flow, input, functor);
      flow.add(map);

      return map.output();
    }
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("FlatMap", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final UnaryFunctor<IN, OUT> functor;

  FlatMap(String name, Flow flow, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
    super(name, flow, input);
    this.functor = functor;
  }

  public UnaryFunctor<IN, OUT> getFunctor() {
    return functor;
  }


}
