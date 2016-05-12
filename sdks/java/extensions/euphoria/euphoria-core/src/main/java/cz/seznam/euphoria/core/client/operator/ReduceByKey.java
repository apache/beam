
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Operator performing state-less aggregation by given reduce function.
 */
public class ReduceByKey<
    IN, KEY, VALUE, OUT, W extends Window<?, ?>>
    extends StateAwareWindowWiseSingleInputOperator<IN, IN, IN, KEY, Pair<KEY, OUT>, W,
        ReduceByKey<IN, KEY, VALUE, OUT, W>> {

  public static class Builder1<IN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <KEY> Builder2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
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
    public <VALUE> Builder3<IN, KEY, VALUE> valueBy(UnaryFunction<IN, VALUE> valueExtractor) {
      return new Builder3<>(input, keyExtractor, valueExtractor);
    }
    public <OUT> Builder4<IN, KEY, IN, OUT> reduceBy(ReduceFunction<IN, OUT> reducer) {
      return new Builder4<>(input, keyExtractor, e-> e, reducer);
    }
    @SuppressWarnings("unchecked")
    public Builder4<IN, KEY, IN, IN> combineBy(CombinableReduceFunction<IN> reducer) {
      return new Builder4<>(input, keyExtractor, e -> e, (ReduceFunction) reducer);
    }
  }
  public static class Builder3<IN, KEY, VALUE> {
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, VALUE> valueExtractor;
    Builder3(Dataset<IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
    }
    public <OUT> Builder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return new Builder4<>(input, keyExtractor, valueExtractor, reducer);
    }
    public Builder4<IN, KEY, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new Builder4<>(input, keyExtractor, valueExtractor, reducer);
    }
  }
  public static class Builder4<IN, KEY, VALUE, OUT> {
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, VALUE> valueExtractor;
    final ReduceFunction<VALUE, OUT> reducer;
    Builder4(Dataset<IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor,
        ReduceFunction<VALUE, OUT> reducer) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.reducer = reducer;
    }
    public <G, L, W extends Window<G, L>> ReduceByKey<IN, KEY, VALUE, OUT, W>
    windowBy(Windowing<IN, G, L, W> windowing) {
      Flow flow = input.getFlow();
      ReduceByKey<IN, KEY, VALUE, OUT, W>
      reduceByKey = new ReduceByKey<>(flow, input, keyExtractor,
          valueExtractor, windowing, reducer);
      return flow.add(reduceByKey);
    }
    public Dataset<Pair<KEY, OUT>> output() {
      return windowBy(BatchWindowing.get()).output();
    }
  }


  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  public static <KEY, IN> GroupReduceByKey.Builder1<KEY, IN> of(
      GroupedDataset<KEY, IN> input) {
    return new GroupReduceByKey.Builder1<>(input);
  }

  private final ReduceFunction<VALUE, OUT> reducer;
  private final UnaryFunction<IN, VALUE> valueExtractor;


  ReduceByKey(Flow flow,
      Dataset<IN> input,
      UnaryFunction<IN, KEY> keyExtractor,
      UnaryFunction<IN, VALUE> valueExtractor,
      Windowing<IN, ?, ?, W> windowing,
      ReduceFunction<VALUE, OUT> reducer) {
    super("ReduceByKey", flow, input, keyExtractor, windowing);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.setPartitioning(new HashPartitioning<>(
        input.getPartitioning().getNumPartitions()));
  }
  
  public ReduceFunction<VALUE, OUT> getReducer() {
    return reducer;
  }

  // state represents the output value
  private class ReduceState extends State<VALUE, Pair<KEY, OUT>> {

    final List<VALUE> reducableValues = new ArrayList<>();
    final boolean isCombinable = reducer instanceof CombinableReduceFunction;
    KEY key;

    ReduceState(KEY key, Collector<Pair<KEY, OUT>> collector) {
      super(collector);
      this.key = key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(VALUE element) {
      reducableValues.add(element);
      combineIfPossible();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() {
      collector.collect(Pair.of(key, reducer.apply(reducableValues)));
    }

    void add(ReduceState other) {
      this.reducableValues.addAll(other.reducableValues);
      combineIfPossible();
    }

    @SuppressWarnings("unchecked")
    private void combineIfPossible() {
      if (isCombinable && reducableValues.size() > 1) {
        OUT val = reducer.apply(reducableValues);
        reducableValues.clear();
        reducableValues.add((VALUE) val);
      }
    }

  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // this can be implemented using ReduceStateByKey

    Flow flow = getFlow();
    Operator<?, ?> reduceState;
    reduceState = new ReduceStateByKey<>(
        flow, input, keyExtractor, valueExtractor,
        windowing,
        ReduceState::new,
        (Iterable<ReduceState> states) -> {
          final ReduceState first;
          Iterator<ReduceState> i = states.iterator();
          first = i.next();
          while (i.hasNext()) {
            first.add(i.next());
          }
          return first;
        });
    return DAG.of(reduceState);
  }


}
