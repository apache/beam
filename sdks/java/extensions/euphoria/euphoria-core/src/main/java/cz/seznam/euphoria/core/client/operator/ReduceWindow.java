
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.util.Pair;

/**
 * Reduce all elements in window.
 */
public class ReduceWindow<
    IN, VALUE, OUT, WLABEL, W extends WindowContext<?, WLABEL>>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, Byte, OUT, WLABEL, W,
            ReduceWindow<IN, VALUE, OUT, WLABEL, W>> {
  
  public static class OfBuilder {
    final String name;
    OfBuilder(String name) {
      this.name = name;
    }
    public <T> ValueBuilder<T> of(Dataset<T> input) {
      return new ValueBuilder<>(name, input);
    }
  }

  public static class ValueBuilder<T> {
    final String name;
    final Dataset<T> input;
    ValueBuilder(String name, Dataset<T> input) {
      this.name = name;
      this.input = input;
    }
    public <VALUE> ReduceBuilder<T, VALUE> valueBy(
        UnaryFunction<T, VALUE> valueExtractor) {
      return new ReduceBuilder<>(name, input, valueExtractor);
    }
    public <OUT> OutputBuilder<T, T, OUT> reduceBy(
        ReduceFunction<T, OUT> reducer) {
      return new OutputBuilder<>(name, input, e -> e, reducer);
    }
    public OutputBuilder<T, T, T> combineBy(
        CombinableReduceFunction<T> reducer) {
      return new OutputBuilder<>(name, input, e -> e, reducer);
    }
  }
  
  public static class ReduceBuilder<T, VALUE> {
    final String name;
    final Dataset<T> input;
    final UnaryFunction<T, VALUE> valueExtractor;
    public ReduceBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor) {
      
      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
    }
    public <OUT> OutputBuilder<T, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return new OutputBuilder<>(name, input, valueExtractor, reducer);
    }
    public OutputBuilder<T, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new OutputBuilder<>(name, input, valueExtractor, reducer);
    }
  }

  public static class OutputBuilder<T, VALUE, OUT>
      extends OptionalMethodBuilder<OutputBuilder<T, VALUE, OUT>> {
    
    final String name;
    final Dataset<T> input;
    final UnaryFunction<T, VALUE> valueExtractor;    
    final ReduceFunction<VALUE, OUT> reducer;
    int numPartitions = -1;
    Windowing<T, ?, ?, ?> windowing;

    public OutputBuilder(
        String name,
        Dataset<T> input,
        UnaryFunction<T, VALUE> valueExtractor,
        ReduceFunction<VALUE, OUT> reducer) {
      this.name = name;
      this.input = input;
      this.valueExtractor = valueExtractor;
      this.reducer = reducer;
    }
    @SuppressWarnings("unchecked")
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      ReduceWindow<T, VALUE, OUT, ?, ?> operator = new ReduceWindow<>(
          name, flow, input, valueExtractor, (Windowing) windowing, reducer, numPartitions);
      flow.add(operator);
      return operator.output();
    }
    public <GROUP, LABEL, W extends WindowContext<GROUP, LABEL>> OutputBuilder<T, VALUE, OUT>
    windowBy(Windowing<T, GROUP, LABEL, W> windowing) {
      this.windowing = windowing;
      return this;
    }

    public OutputBuilder<T, VALUE, OUT> setNumPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }
  }

  public static <T> ValueBuilder<T> of(Dataset<T> input) {
    return new ValueBuilder<>("ReduceWindow", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }


  final ReduceFunction<VALUE, OUT> reducer;
  final UnaryFunction<IN, VALUE> valueExtractor;

  static final Byte B_ZERO = (byte) 0;

  private ReduceWindow(
      String name,
      Flow flow,
      Dataset<IN> input,
      UnaryFunction<IN, VALUE> valueExtractor,
      Windowing<IN, ?, WLABEL, W> windowing,
      ReduceFunction<VALUE, OUT> reducer,
      int numPartitions) {
    super(name, flow, input, e -> B_ZERO, windowing,
        new Partitioning<Byte>() {
          @Override
          public Partitioner<Byte> getPartitioner() {
            return e -> 0;
          }
          @Override
          public int getNumPartitions() {
            return numPartitions > 0
                ? numPartitions : input.getPartitioning().getNumPartitions();
          }
    });
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
  }

  public ReduceFunction<VALUE, OUT> getReducer() {
    return reducer;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // implement this operator via `ReduceByKey`
    ReduceByKey<IN, IN, Byte, VALUE, Void, OUT, WLABEL, W , Pair<Void, OUT>> reduceByKey;
    reduceByKey = new ReduceByKey<>(
        getName() + "::ReduceByKey", getFlow(), input,
        getKeyExtractor(), valueExtractor,
        windowing, reducer, partitioning);
    Dataset<Pair<Void, OUT>> output = reduceByKey.output();

    MapElements<Pair<Void, OUT>, OUT> format = new MapElements<>(
        getName() + "::MapElements", getFlow(), output, Pair::getSecond);

    return DAG.of(reduceByKey, format);
  }




}
