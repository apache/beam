
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Objects;

/**
 * Repartition input to some other number of partitions.
 */
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
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

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
