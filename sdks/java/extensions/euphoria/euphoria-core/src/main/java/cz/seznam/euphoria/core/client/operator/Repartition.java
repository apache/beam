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

import cz.seznam.euphoria.core.annotation.operator.Basic;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Objects;

/**
 * Repartition input to some other number of partitions.
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

    @Override
    public Dataset<IN> output() {
      Flow flow = input.getFlow();
      Repartition<IN> repartition =
              new Repartition<>(name, flow, input, getPartitioning());
      flow.add(repartition);
      return repartition.output();
    }
  }

  public static <IN> OutputBuilder<IN> of(Dataset<IN> input) {
    return new OutputBuilder<>("Repartition", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final Partitioning<IN> partitioning;

  Repartition(String name, Flow flow, Dataset<IN> input,
      final Partitioning<IN> partitioning) {
    super(name, flow, input);
    this.partitioning = partitioning;
  }

  @Override
  public Partitioning<IN> getPartitioning() {
    return partitioning;
  }


}
