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
 * Operator outputting distinct (based on {@link Object#equals}) elements.
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

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> WindowingBuilder<IN, IN> of(Dataset<IN> input) {
      return new WindowingBuilder<>(name, input, e -> e);
    }
  }

  public static class WindowingBuilder<IN, ELEM>
          extends PartitioningBuilder<ELEM, WindowingBuilder<IN, ELEM>>
          implements Builders.WindowBy<IN>,
                     Builders.Output<ELEM>,
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

    /**
     * Optionally specifies a function to transform the input elements into
     * another type among which to find the distincts.<p>
     *
     * This is, while windowing will be applied on basis of original input
     * elements, the distinct operator will be carried out on the
     * transformed elements.
     *
     * @param <ELEM> the type of the transformed elements
     *
     * @param mapper a transform function applied to input element
     *
     * @return the next builder to complete the setup of the {@link Distinct}
     *          operator
     */
    public <ELEM> WindowingBuilder<IN, ELEM> mapped(UnaryFunction<IN, ELEM> mapper) {
      return new WindowingBuilder<>(name, input, mapper);
    }
    
    @Override
    public <W extends Window> OutputBuilder<IN, ELEM, W>
    windowBy(Windowing<IN, W> windowing) {
      return new OutputBuilder<>(name, input, mapper, this, windowing);
    }
    
    @Override
    public Dataset<ELEM> output() {
      return new OutputBuilder<>(name, input, mapper, this, null).output();
    }
  }

  public static class OutputBuilder<IN, ELEM, W extends Window>
      extends PartitioningBuilder<ELEM, OutputBuilder<IN, ELEM, W>>
      implements Builders.Output<ELEM>
  {
    private final String name;
    private final Dataset<IN> input;
    @Nullable
    private final UnaryFunction<IN, ELEM> mapper;
    @Nullable
    private final Windowing<IN, W> windowing;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  @Nullable UnaryFunction<IN, ELEM> mapper,
                  PartitioningBuilder<ELEM, ?> partitioning,
                  @Nullable Windowing<IN, W> windowing) {

      super(partitioning);
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.mapper = mapper;
      this.windowing = windowing;
    }

    @Override
    public Dataset<ELEM> output() {
      Flow flow = input.getFlow();
      Distinct<IN, ELEM, W> distinct = new Distinct<>(
          name, flow, input, mapper, getPartitioning(), windowing);
      flow.add(distinct);
      return distinct.output();
    }
  }

  /**
   * Starts building a nameless {@link Distinct} operator to process
   * the given input dataset.
   *
   * @param <IN> the type of elements of the input dataset
   *
   * @param input the input data set to be processed
   *
   * @return a builder to complete the setup of the new operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> WindowingBuilder<IN, IN> of(Dataset<IN> input) {
    return new WindowingBuilder<>("Distinct", input, e -> e);
  }

  /**
   * Starts building a named {@link Distinct} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  Distinct(String name,
           Flow flow,
           Dataset<IN> input,
           UnaryFunction<IN, ELEM> mapper,
           Partitioning<ELEM> partitioning,
           @Nullable Windowing<IN, W> windowing) {

    super(name, flow, input, mapper, windowing, partitioning);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<IN, ELEM, Void, Void, W> reduce =
        new ReduceByKey<>(name,
            flow, input, getKeyExtractor(), e -> null,
            windowing,
            (CombinableReduceFunction<Void>) e -> null, partitioning);

    reduce.setPartitioning(getPartitioning());
    MapElements format = new MapElements<>(
        getName() + "::" + "Map", flow, reduce.output(), Pair::getFirst);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }
}
