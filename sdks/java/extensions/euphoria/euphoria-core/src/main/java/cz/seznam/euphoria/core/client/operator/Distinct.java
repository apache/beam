
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Objects;

/**
 * Operator outputting distinct (based on equals) elements.
 */
public class Distinct<IN, W extends Window<?, ?>>
    extends StateAwareWindowWiseSingleInputOperator<IN, IN, IN, IN, IN, W, Distinct<IN, W>> {

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
  {
    private final String name;
    private final Dataset<IN> input;
    private Windowing<IN, ?, ?, ?> windowing;
    OutputBuilder(String name, Dataset<IN> input) {
      // define default partitioning
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <W extends Window<?, ?>> OutputBuilder<IN> windowBy(
        Windowing<IN, ?, ?, W> windowing)
    {
      this.windowing = Objects.requireNonNull(windowing);
      return this;
    }
    public Dataset<IN> output() {
      Flow flow = input.getFlow();
      Distinct<IN, ?> distinct = new Distinct<>(name, flow, input, windowing, getPartitioning());
      flow.add(distinct);

      return distinct.output();
    }
  }

  public static <IN> OutputBuilder<IN> of(Dataset<IN> input) {
    return new OutputBuilder<>("Distinct", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  Distinct(String name,
           Flow flow,
           Dataset<IN> input,
           Windowing<IN, ?, ?, W> windowing,
           Partitioning<IN> partitioning)
  {
    super(name, flow, input, e -> e, windowing, partitioning);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {

    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<IN, IN, IN, Void, IN, Void, W> reduce;
    reduce = new ReduceByKey<>(name,
            flow, input, e -> e, e -> null,
            windowing,
            (CombinableReduceFunction<Void>) e -> null, partitioning);

    reduce.setPartitioning(getPartitioning());
    Dataset<Pair<IN, Void>> reduced = reduce.output();
    name = getName() + "::" + "Map";
    MapElements<Pair<IN, Void>, IN> format =
            new MapElements<>(name, flow, reduced, Pair::getFirst);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }



}
