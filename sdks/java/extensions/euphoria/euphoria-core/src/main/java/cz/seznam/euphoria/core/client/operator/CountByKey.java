/**
 * Copyright 2016 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Objects;

/**
 * Operator counting elements with same key.
 */
@Derived(
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class CountByKey<IN, KEY, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, KEY, Pair<KEY, Long>, W, CountByKey<IN, KEY, W>> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> ByBuilder<IN> of(Dataset<IN> input) {
      return new ByBuilder<>(name, input);
    }
  }

  public static class ByBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;
    ByBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <KEY> WindowingBuilder<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new WindowingBuilder<>(name, input, keyExtractor);
    }
  }
  public static class WindowingBuilder<IN, KEY>
          extends PartitioningBuilder<KEY, WindowingBuilder<IN, KEY>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;

    WindowingBuilder(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      // define default partitioning
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    public <W extends Window> OutputBuilder<IN, KEY, W>
    windowBy(Windowing<IN, W> windowing, UnaryFunction<IN, Long> eventTimeAssigner) {
      return new OutputBuilder<>(name, input, keyExtractor, windowing, eventTimeAssigner, this);
    }

    public <W extends Window> OutputBuilder<IN, KEY, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }

    public Dataset<Pair<KEY, Long>> output() {
      return windowBy(null, null).output();
    }
  }
  public static class OutputBuilder<IN, KEY, W extends Window>
          extends PartitioningBuilder<KEY, OutputBuilder<IN, KEY, W>> {

    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final Windowing<IN, W> windowing;
    private final UnaryFunction<IN, Long> eventTimeAssigner;


    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, KEY> keyExtractor,
                  Windowing<IN, W> windowing /* optional */,
                  UnaryFunction<IN, Long> eventTimeAssigner /* optional */,
                  PartitioningBuilder<KEY, ?> partitioning) {

      //initialize partitioning
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    public Dataset<Pair<KEY, Long>> output() {
      Flow flow = input.getFlow();
      CountByKey<IN, KEY, W> count = new CountByKey<>(
              name, flow, input, keyExtractor,
              windowing, eventTimeAssigner, getPartitioning());
      flow.add(count);
      return count.output();
    }
  }

  public static <IN> ByBuilder<IN> of(Dataset<IN> input) {
    return new ByBuilder<>("CountByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  CountByKey(String name,
      Flow flow,
      Dataset<IN> input,
      UnaryFunction<IN, KEY> extractor,
      Windowing<IN, W> windowing /* optional */,
      UnaryFunction<IN, Long> eventTimeAssigner /* optional */,
      Partitioning<KEY> partitioning) {

    super(name, flow, input, extractor, windowing, eventTimeAssigner,  partitioning);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    SumByKey<IN, KEY, W> sum = new SumByKey<>(
            getName(),
            input.getFlow(),
            input,
            keyExtractor,
            e -> 1L,
            windowing,
            eventTimeAssigner,
            partitioning);
    return DAG.of(sum);
  }
}
