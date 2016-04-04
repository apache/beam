
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;

/**
 * Operator counting elements with same key.
 */
public class CountByKey<IN, KEY, W extends Window<?, W>,
    TYPE extends Dataset<Pair<KEY, Long>>>
    extends StateAwareWindowWiseSingleInputOperator<IN, KEY, Pair<KEY, Long>, W, TYPE,
        CountByKey<IN, KEY, W, TYPE>> {

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
    public <W extends Window<?, W>> CountByKey<IN, KEY, W, Dataset<Pair<KEY, Long>>>
    windowBy(Windowing<IN, ?, W> windowing) {
      Flow flow = input.getFlow();
      return flow.add(new CountByKey<>(flow, input,
          keyExtractor, windowing, new HashPartitioning<>()));
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  CountByKey(Flow flow,
      Dataset<IN> input,
      UnaryFunction<IN, KEY> extractor,
      Windowing<IN, ?, W> windowing,
      Partitioning<KEY> partitioning) {

    super("CountByKey", flow, input, extractor, windowing, partitioning);
  }

  @Override
  public DAG<Operator<?, ?, ?>> getBasicOps() {
    SumByKey<IN, KEY, W, Dataset<Pair<KEY, Long>>> sum = new SumByKey<>(
        input.getFlow(),
        input,
        keyExtractor,
        e -> 1L,
        windowing,
        partitioning);
    return DAG.of(sum);
  }



}
