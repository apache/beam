
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Objects;

/**
 * Operator outputting distinct (based on equals) elements.
 */
public class Distinct<IN, WLABEL, W extends WindowContext<?, WLABEL>, OUT>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, IN, OUT, WLABEL, W, Distinct<IN, WLABEL, W, OUT>>
{

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> WindowingBuilder<IN> of(Dataset<IN> input) {
      return new WindowingBuilder<>(name, input);
    }
  }

  public static class WindowingBuilder<IN>
          extends PartitioningBuilder<IN, WindowingBuilder<IN>>
          implements cz.seznam.euphoria.core.client.operator.OutputBuilder<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    WindowingBuilder(String name, Dataset<IN> input) {
      // define default partitioning
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <WLABEL, W extends WindowContext<?, WLABEL>> OutputBuilder<IN, WLABEL, W>
    windowBy(Windowing<IN, ?, WLABEL, W> windowing) {
      return new OutputBuilder<>(this, windowing);
    }
    @Override
    public Dataset<IN> output() {
      return new OutputBuilder<>(this, Batch.get()).output();
    }
  }

  public static class OutputBuilder<IN, WLABEL, W extends WindowContext<?, WLABEL>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<IN>
  {
    private final WindowingBuilder<IN> prev;
    private final Windowing<IN, ?, WLABEL, W> windowing;
    OutputBuilder(WindowingBuilder<IN> prev, Windowing<IN, ?, WLABEL, W> windowing) {
      this.prev = Objects.requireNonNull(prev);
      this.windowing = Objects.requireNonNull(windowing);
    }
    @SuppressWarnings("unchecked")
    @Override
    public Dataset<IN> output() {
      return outputImpl(false);
    }
    @SuppressWarnings("unchecked")
    public Dataset<Pair<WLABEL, IN>> outputWindowed() {
      return outputImpl(true);
    }
    private Dataset outputImpl(boolean windowedOutput) {
      Flow flow = prev.input.getFlow();
      Distinct<IN, WLABEL, W, ?> distinct = new Distinct<>(
          prev.name, flow, prev.input, windowing, prev.getPartitioning(), windowedOutput);
      flow.add(distinct);
      return distinct.output();
    }
  }

  public static <IN> WindowingBuilder<IN> of(Dataset<IN> input) {
    return new WindowingBuilder<>("Distinct", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final boolean windowedOutput;

  Distinct(String name,
           Flow flow,
           Dataset<IN> input,
           Windowing<IN, ?, WLABEL, W> windowing,
           Partitioning<IN> partitioning,
           boolean windowedOutput)
  {
    super(name, flow, input, e -> e, windowing, partitioning);
    this.windowedOutput = windowedOutput;
  }

  public boolean isWindowedOutput() {
    return windowedOutput;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<IN, IN, IN, Void, IN, Void, WLABEL, W, WindowedPair<WLABEL, IN, Void>>
        reduce;
    reduce = new ReduceByKey<>(name,
            flow, input, e -> e, e -> null,
            windowing,
            (CombinableReduceFunction<Void>) e -> null, partitioning);

    reduce.setPartitioning(getPartitioning());
    Dataset<WindowedPair<WLABEL, IN, Void>> reduced = reduce.output();
    name = getName() + "::" + "Map";
    MapElements format =
        windowedOutput
          ? new MapElements<>(
              name, flow, reduced, t -> Pair.of(t.getWindowLabel(), t.getFirst()))
          : new MapElements<>(
            name, flow, reduced, WindowedPair::getFirst);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }
}
