package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;

import java.util.Objects;

/**
 * Operator for summing of elements by key.
 */
@Derived(
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class SumByKey<
    IN, KEY, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, KEY, Pair<KEY, Long>, W,
        SumByKey<IN, KEY, W>> {
  
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
    public <KEY> ByBuilder2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new ByBuilder2<>(name, input, keyExtractor);
    }
  }
  public static class ByBuilder2<IN, KEY>
      extends PartitioningBuilder<KEY, ByBuilder2<IN, KEY>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Pair<KEY, Long>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private UnaryFunction<IN, Long> valueExtractor = e -> 1L;
    ByBuilder2(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }
    public ByBuilder2<IN, KEY> valueBy(UnaryFunction<IN, Long> valueExtractor) {
      this.valueExtractor = valueExtractor;
      return this;
    }
    public <W extends Window> OutputBuilder<IN, KEY, W>
    windowBy(Windowing<IN, W> windowing)
    {
      return new OutputBuilder<>(
              name, input, keyExtractor, valueExtractor, windowing, this);
    }
    @Override
    public Dataset<Pair<KEY, Long>> output() {
      // use default windowing
      return new OutputBuilder<>(name, input,
          keyExtractor, valueExtractor, Batch.get(), this)
          .output();
    }
  }
  public static class OutputBuilder<IN, KEY, W extends Window>
      extends PartitioningBuilder<KEY, OutputBuilder<IN, KEY, W>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Pair<KEY, Long>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, Long> valueExtractor;
    private final Windowing<IN, W> windowing; /* may be null */
    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, KEY> keyExtractor,
                  UnaryFunction<IN, Long> valueExtractor,
                  Windowing<IN, W> windowing,
                  PartitioningBuilder<KEY, ?> partitioning)
    {
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.windowing = Objects.requireNonNull(windowing);
    }
    @Override
    public Dataset<Pair<KEY, Long>> output() {
      Flow flow = input.getFlow();
      SumByKey<IN, KEY, W> sumByKey =
          new SumByKey<>(name, flow, input,
              keyExtractor, valueExtractor, windowing, getPartitioning());
      flow.add(sumByKey);
      return sumByKey.output();
    }
  }

  public static <IN> ByBuilder<IN> of(Dataset<IN> input) {
    return new ByBuilder<>("SumByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final UnaryFunction<IN, Long> valueExtractor;

  SumByKey(String name,
           Flow flow,
           Dataset<IN> input,
           UnaryFunction<IN, KEY> keyExtractor,
           UnaryFunction<IN, Long> valueExtractor,
           Windowing<IN, W> windowing,
           Partitioning<KEY> partitioning)
  {
    super(name, flow, input, keyExtractor, windowing, partitioning);
    this.valueExtractor = valueExtractor;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    ReduceByKey<IN, IN, KEY, Long, KEY, Long, W> reduceByKey =
        new ReduceByKey<>(getName(), input.getFlow(), input,
        keyExtractor, valueExtractor, windowing, Sums.ofLongs(), getPartitioning());
    return DAG.of(reduceByKey);
  }




}
