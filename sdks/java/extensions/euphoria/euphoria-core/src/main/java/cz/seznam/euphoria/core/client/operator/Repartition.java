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
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
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
{

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> OutputBuilder<IN> of(Dataset<IN> input) {
      return new OutputBuilder<>(name, input);
    }
  }

  private static class WithPartitioner<IN> implements Partitioning<IN> {

    private final Partitioning<IN> parent;
    private final Partitioner<IN> partitioner;
    WithPartitioner(Partitioning<IN> parent, Partitioner<IN> partitioner) {
      this.parent = parent;
      this.partitioner = partitioner;
    }

    @Override
    public int getNumPartitions() {
      return parent.getNumPartitions();
    }

    @Override
    public Partitioner<IN> getPartitioner() {
      return partitioner;
    }

  }

  private static class WithNumPartitions<IN> implements Partitioning<IN> {

    private final Partitioning<IN> parent;
    private final int numPartitions;
    WithNumPartitions(Partitioning<IN> parent, int numPartitions) {
      this.parent = parent;
      this.numPartitions = numPartitions;
    }

    @Override
    public int getNumPartitions() {
      return numPartitions;
    }

    @Override
    public Partitioner<IN> getPartitioner() {
      return parent.getPartitioner();
    }

  }


  public static class OutputBuilder<IN> implements Builders.Output<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    private Partitioning<IN> partitioning;
    OutputBuilder(String name, Dataset<IN> input) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.partitioning = new DefaultPartitioning<>();
    }

    OutputBuilder<IN> setNumPartitions(int numPartitions) {
      this.partitioning = new WithNumPartitions<>(this.partitioning, numPartitions);
      return this;
    }

    OutputBuilder<IN> setPartitioner(Partitioner<IN> partitioner) {
      this.partitioning = new WithPartitioner<>(this.partitioning, partitioner);
      return this;
    }

    @Override
    public Dataset<IN> output() {
      Flow flow = input.getFlow();
      Repartition<IN> repartition =
              new Repartition<>(name, flow, input, partitioning);
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
  public Partitioning<IN> getPartitioning() {
    return partitioning;
  }
}