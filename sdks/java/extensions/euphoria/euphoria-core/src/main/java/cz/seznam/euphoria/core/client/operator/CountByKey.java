
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
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
@Derived(basic = ReduceByKey.class)
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
          extends PartitioningBuilder<KEY, WindowingBuilder<IN, KEY>>
  {
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
    windowBy(Windowing<IN, W> windowing)
    {
      return new OutputBuilder<>(this, windowing);
    }
    public Dataset<Pair<KEY, Long>> output() {
      return new OutputBuilder<>(this, Batch.get()).output();
    }
  }
  public static class OutputBuilder<IN, KEY, W extends Window> {
    private final WindowingBuilder<IN, KEY> prev;
    private final Windowing<IN, W> windowing;
    OutputBuilder(WindowingBuilder<IN, KEY> prev,
                  Windowing<IN, W> windowing)
    {
      this.prev = Objects.requireNonNull(prev);
      this.windowing = Objects.requireNonNull(windowing);
    }

    public Dataset<Pair<KEY, Long>> output() {
      Flow flow = prev.input.getFlow();
      CountByKey<IN, KEY, W> count =
          new CountByKey<>(
              prev.name, flow, prev.input,
              prev.keyExtractor, windowing, prev.getPartitioning());
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
      Windowing<IN, W> windowing,
      Partitioning<KEY> partitioning)
  {
    super(name, flow, input, extractor, windowing, partitioning);
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
            partitioning);
    return DAG.of(sum);
  }
}
