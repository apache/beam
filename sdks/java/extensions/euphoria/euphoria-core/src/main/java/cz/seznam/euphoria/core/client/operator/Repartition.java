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
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Objects;

/**
 * Repartition the input dataset. Repartioning allows 1) to redistribute
 * a dataset's elements across their partitions and/or 2) to define the
 * number of partitions of a dataset.
 *
 * Example:
 *
 * <pre>{@code
 *   Dataset<String> strings = ...;
 *   strings = Repartition
 *      .of(strings)
 *      .setNumPartitions(10)
 *      .setPartitioner(new HashPartitioner<>())
 *      .output();
 * }</pre>
 *
 * Here, the input dataset is repartitioned into 10 partitions, distributing
 * the elements based on their hash code as computed by `String#hashCode`.<p>
 *
 * {@code #setNumPartitions} is optional and will default to the number of
 * partitions of the input dataset. Effectively merely redistributing the
 * dataset elements according to the specified partitioner.<p>
 *
 * Also {@code #setPartitioner} is optional, defaulting to
 * {@link cz.seznam.euphoria.core.client.dataset.partitioning.HashPartitioner}.
 *
 * Note: as with all Euphoria operators, you must continue to use the
 * repartition operator's output dataset to make the operation effective.
 */
@Basic(
    state = StateComplexity.ZERO,
    repartitions = 1
)
public class Repartition<IN>
    extends ElementWiseOperator<IN, IN>
    implements PartitioningAware<IN>
{

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    /**
     * Specifies the input dataset to be repartitioned.
     *
     * @param <IN> the type of elements in the input dataset
     *
     * @param input the input dataset to process
     *
     * @return the next builder to complete the setup of the repartition operator
     */
    public <IN> OutputBuilder<IN> of(Dataset<IN> input) {
      return new OutputBuilder<>(name, input);
    }
  }

  public static class OutputBuilder<IN>
      extends PartitioningBuilder<IN, OutputBuilder<IN>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    OutputBuilder(String name, Dataset<IN> input) {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    /**
     * Finalizes the setup of the {@link Repartition} operator and retrieves
     * the dataset representing the repartitioned input dataset.
     *
     * @return the dataset represeting the repartitioned input
     */
    @Override
    public Dataset<IN> output() {
      Flow flow = input.getFlow();
      Repartition<IN> repartition =
              new Repartition<>(name, flow, input, getPartitioning());
      flow.add(repartition);
      return repartition.output();
    }
  }

  /**
   * Starts building a nameless {@link Repartition} operator over the given
   * input dataset.
   *
   * @param <IN> the type of elements in the input dataset
   *
   * @param input the input dataset to process
   *
   * @return a builder to complete the setup of the {@link Repartition} operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> OutputBuilder<IN> of(Dataset<IN> input) {
    return new OutputBuilder<>("Repartition", input);
  }

  /**
   * Starts building a named {@link Repartition} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new {@link Repartition} operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final Partitioning<IN> partitioning;

  Repartition(String name, Flow flow, Dataset<IN> input,
      final Partitioning<IN> partitioning) {
    super(name, flow, input);
    this.partitioning = partitioning;
  }

  /**
   * Retrieves the partitioning information according which this operators
   * input dataset is to be redistributed.
   *
   * @return the partitioning schema of this {@link Repartition} operator
   */
  @Override
  public Partitioning<IN> getPartitioning() {
    return partitioning;
  }
}