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

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;

import java.util.Objects;

@Derived(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class MapElements<IN, OUT> extends ElementWiseOperator<IN, OUT> {

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

    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunction<IN, OUT> mapper) {
      return new OutputBuilder<>(name, input, mapper);
    }
  }

  public static class OutputBuilder<IN, OUT>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<OUT> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, OUT> mapper;

    OutputBuilder(String name, Dataset<IN> input, UnaryFunction<IN, OUT> mapper) {
      this.name = name;
      this.input = input;
      this.mapper = mapper;
    }

    @Override
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      MapElements<IN, OUT> map = new MapElements<>(name, flow, input, mapper);
      flow.add(map);

      return map.output();
    }
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("Map", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final UnaryFunction<IN, OUT> mapper;

  MapElements(String name, Flow flow, Dataset<IN> input, UnaryFunction<IN, OUT> mapper) {
    super(name, flow, input);
    this.mapper = mapper;
  }

  /**
   * This is not a basic operator. It can be straightforwardly implemented
   * by using {@code FlatMap} operator.
   * @return the operator chain representing this operation including FlatMap
   */
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(
        // do not use the client API here, because it modifies the Flow!
        new FlatMap<IN, OUT>(getName(), getFlow(), input,
            (i, c) -> c.collect(mapper.apply(i))));
  }


}
