
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Objects;

/**
 * Operator outputting distinct (based on equals) elements.
 */
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
    private final UnaryFunction<IN, ELEM> mapper;
    WindowingBuilder(
        String name,
        Dataset<IN> input,
        UnaryFunction<IN, ELEM> mapper) {

      // define default partitioning
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.mapper = mapper;
    }
    public <ELEM> WindowingBuilder<IN, ELEM> mapped(UnaryFunction<IN, ELEM> mapper) {
      return new WindowingBuilder<>(name, input, mapper);
    }
    public <W extends Window> OutputBuilder<IN, ELEM, W>
    windowBy(Windowing<IN, W> windowing) {
      return new OutputBuilder<>(this, windowing);
    }
    @Override
    public Dataset<ELEM> output() {
      return new OutputBuilder<>(this, Batch.get()).output();
    }
  }

  public static class OutputBuilder<IN, ELEM, W extends Window>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<ELEM>
  {
    private final WindowingBuilder<IN, ELEM> prev;
    private final Windowing<IN, W> windowing;

    OutputBuilder(WindowingBuilder<IN, ELEM> prev, Windowing<IN, W> windowing) {
      this.prev = Objects.requireNonNull(prev);
      this.windowing = Objects.requireNonNull(windowing);
    }

    @Override
    public Dataset<ELEM> output() {
      Flow flow = prev.input.getFlow();
      Distinct<IN, ELEM, W> distinct;
      distinct = new Distinct<>(
          prev.name, flow, prev.input, prev.mapper, windowing,
          prev.getPartitioning());
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
           Windowing<IN, W> windowing,
           Partitioning<ELEM> partitioning)
  {
    super(name, flow, input, mapper, windowing, partitioning);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<IN, IN, ELEM, Void, IN, Void, W>
        reduce;
    reduce = new ReduceByKey<>(name,
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
