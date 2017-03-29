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

import cz.seznam.euphoria.core.annotation.operator.Recommended;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Operator outputting distinct (based on equals) elements.
 */
@Recommended(
    reason =
        "Might be useful to override the default "
      + "implementation because of performance reasons"
      + "(e.g. using bloom filters), which might reduce the space complexity",
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class Distinct<IN, ELEM, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, ELEM, ELEM, W, Distinct<IN, ELEM, W>>
{

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> WindowingBuilder<IN, IN> of(Dataset<IN> input) {
      return new WindowingBuilder<>(name, input, e -> e);
    }
  }

  public static class WindowingBuilder<IN, ELEM>
          extends PartitioningBuilder<ELEM, WindowingBuilder<IN, ELEM>>
          implements cz.seznam.euphoria.core.client.operator.OutputBuilder<ELEM>,
                     OptionalMethodBuilder<WindowingBuilder<IN, ELEM>>
  {
    private final String name;
    private final Dataset<IN> input;
    @Nullable
    private final UnaryFunction<IN, ELEM> mapper;

    WindowingBuilder(
        String name,
        Dataset<IN> input,
        @Nullable UnaryFunction<IN, ELEM> mapper) {

      // define default partitioning
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.mapper = mapper;
    }
    public <ELEM> WindowingBuilder<IN, ELEM> mapped(UnaryFunction<IN, ELEM> mapper) {
      return new WindowingBuilder<>(name, input, mapper);
    }
    public <W extends Window> OutputBuilder<IN, ELEM, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }
    public <W extends Window> OutputBuilder<IN, ELEM, W>
    windowBy(Windowing<IN, W> windowing, ExtractEventTime<IN> eventTimeAssigner) {
      return new OutputBuilder<>(name, input, mapper, this, windowing, eventTimeAssigner);
    }
    @Override
    public Dataset<ELEM> output() {
      return new OutputBuilder<>(name, input, mapper, this, null, null).output();
    }
  }

  public static class OutputBuilder<IN, ELEM, W extends Window>
      extends PartitioningBuilder<ELEM, OutputBuilder<IN, ELEM, W>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<ELEM>
  {
    private final String name;
    private final Dataset<IN> input;
    @Nullable
    private final UnaryFunction<IN, ELEM> mapper;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final ExtractEventTime<IN> eventTimeAssigner;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  @Nullable UnaryFunction<IN, ELEM> mapper,
                  PartitioningBuilder<ELEM, ?> partitioning,
                  @Nullable Windowing<IN, W> windowing,
                  @Nullable ExtractEventTime<IN> eventTimeAssigner) {

      super(partitioning);
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.mapper = mapper;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<ELEM> output() {
      Flow flow = input.getFlow();
      Distinct<IN, ELEM, W> distinct = new Distinct<>(
          name, flow, input, mapper, getPartitioning(),
              windowing, eventTimeAssigner);
      flow.add(distinct);
      return distinct.output();
    }
  }

  public static <IN> WindowingBuilder<IN, IN> of(Dataset<IN> input) {
    return new WindowingBuilder<>("Distinct", input, e -> e);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  Distinct(String name,
           Flow flow,
           Dataset<IN> input,
           UnaryFunction<IN, ELEM> mapper,
           Partitioning<ELEM> partitioning,
           @Nullable Windowing<IN, W> windowing,
           @Nullable ExtractEventTime<IN> eventTimeAssigner) {

    super(name, flow, input, mapper, windowing, eventTimeAssigner, partitioning);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<IN, IN, ELEM, Void, IN, Void, W>
        reduce;
    reduce = new ReduceByKey<>(name,
            flow, input, getKeyExtractor(), e -> null,
            windowing, eventTimeAssigner,
            (CombinableReduceFunction<Void>) e -> null, partitioning);

    reduce.setPartitioning(getPartitioning());
    MapElements format = new MapElements<>(
        getName() + "::" + "Map", flow, reduce.output(), Pair::getFirst);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }
}
