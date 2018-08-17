
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;

/**
 * Operator for summing of elements by key.
 */
public class SumByKey<IN, KEY, W extends Window<?, ?>>
    extends StateAwareWindowWiseSingleInputOperator<IN, IN, IN, KEY, Pair<KEY, Long>, W,
        SumByKey<IN, KEY, W>> {

  public static class Builder1<IN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <KEY> Builder2<IN, KEY> by(UnaryFunction<IN, KEY> keyExtractor) {
      return new Builder2<>(input, keyExtractor);
    }
  }
  public static class Builder2<IN, KEY> {
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    Builder2(Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
    }
    public Builder3<IN, KEY> valueBy(UnaryFunction<IN, Long> valueExtractor) {
      return new Builder3<>(input, keyExtractor, valueExtractor);
    }
    public <W extends Window<?, ?>> SumByKey<IN, KEY, W>
    windowBy(Windowing<IN, ?, ?, W> windowing) {
      Flow flow = input.getFlow();
      return flow.add(new SumByKey<>(flow, input,
          keyExtractor, e -> 1L, windowing, new HashPartitioning<>()));
    }
    public Dataset<Pair<KEY, Long>> output() {
      return windowBy(BatchWindowing.get()).output();
    }

  }
  public static class Builder3<IN, KEY> {
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, Long> valueExtractor;
    Builder3(Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, Long> valueExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
    }
    public <W extends Window<?, ?>> SumByKey<IN, KEY, W>
    windowBy(Windowing<IN, ?, ?, W> windowing) {
      Flow flow = input.getFlow();
      return flow.add(new SumByKey<>(flow, input,
          keyExtractor, valueExtractor, windowing, new HashPartitioning<>()));
    }
    public Dataset<Pair<KEY, Long>> output() {
      return windowBy(BatchWindowing.get()).output();
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  private final UnaryFunction<IN, Long> valueExtractor;

  SumByKey(Flow flow,
      Dataset<IN> input,
      UnaryFunction<IN, KEY> keyExtractor,
      UnaryFunction<IN, Long> valueExtractor,
      Windowing<IN, ?, ?, W> windowing,
      Partitioning<KEY> partitioning) {

    super("CountByKey", flow, input, keyExtractor, windowing, partitioning);
    this.valueExtractor = valueExtractor;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    ReduceByKey<IN, KEY, Long, Long, W> reduceByKey = new ReduceByKey<>(
        input.getFlow(), input,
        keyExtractor, valueExtractor, windowing, Sums.ofLongs());
    return DAG.of(reduceByKey);
  }




}
