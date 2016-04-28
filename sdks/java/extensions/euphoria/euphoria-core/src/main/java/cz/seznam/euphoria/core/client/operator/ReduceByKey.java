
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.PCollection;
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
    IN, KEY, VALUE, OUT, W extends Window<?>, TYPE extends Dataset<Pair<KEY, OUT>>>
    extends StateAwareWindowWiseSingleInputOperator<IN, IN, IN, KEY, Pair<KEY, OUT>, W, TYPE,
        ReduceByKey<IN, KEY, VALUE, OUT, W, TYPE>> {

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
    public <W extends Window<?>> ReduceByKey<IN, KEY, VALUE, OUT, W, Dataset<Pair<KEY, OUT>>>
    windowBy(Windowing<IN, ?, W> windowing) {
      Flow flow = input.getFlow();
      ReduceByKey<IN, KEY, VALUE, OUT, W, Dataset<Pair<KEY, OUT>>>
      reduceByKey = new ReduceByKey<>(flow, input, keyExtractor,
          valueExtractor, windowing, reducer);
      return flow.add(reduceByKey);
    }
  }


  public static class BuilderBatch1<IN> {
    final PCollection<IN> input;
    BuilderBatch1(PCollection<IN> input) {
      this.input = input;
    }
    public <KEY> BuilderBatch2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new BuilderBatch2<>(input, keyExtractor);
    }
  }
  public static class BuilderBatch2<IN, KEY> {
    final PCollection<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    BuilderBatch2(PCollection<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
    }
    public <VALUE> BuilderBatch3<IN, KEY, VALUE> valueBy(
        UnaryFunction<IN, VALUE> valueExtractor) {
      return new BuilderBatch3<>(input, keyExtractor, valueExtractor);
    }
    public <OUT>
    ReduceByKey<IN, KEY, IN, OUT, BatchWindowing.BatchWindow, PCollection<Pair<KEY, OUT>>>
    reduceBy(ReduceFunction<IN, OUT> reducer) {
      Flow flow = input.getFlow();
      return flow.add(new ReduceByKey<>(
          flow, input, keyExtractor, e -> e, reducer));
    }
    public
    ReduceByKey<IN, KEY, IN, IN, BatchWindowing.BatchWindow, PCollection<Pair<KEY, IN>>>
    combineBy(CombinableReduceFunction<IN> reducer) {
      Flow flow = input.getFlow();
      return flow.add(new ReduceByKey<>(
          flow, input, keyExtractor, e -> e, reducer));
    }
  }
  public static class BuilderBatch3<IN, KEY, VALUE> {
    final PCollection<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, VALUE> valueExtractor;
    BuilderBatch3(PCollection<IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
    }
    public <OUT>
    ReduceByKey<IN, KEY, VALUE, OUT, BatchWindowing.BatchWindow, PCollection<Pair<KEY, OUT>>>
    reduceBy(ReduceFunction<VALUE, OUT> reducer) {
      Flow flow = input.getFlow();
      return flow.add(new ReduceByKey<>(flow, input, keyExtractor,
          valueExtractor, reducer));
    }
    public
    ReduceByKey<IN, KEY, VALUE, VALUE, BatchWindowing.BatchWindow, PCollection<Pair<KEY, VALUE>>>
    combineBy(CombinableReduceFunction<VALUE> reducer) {
      Flow flow = input.getFlow();
      return flow.add(new ReduceByKey<>(flow, input, keyExtractor,
          valueExtractor, reducer));
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  public static <IN> BuilderBatch1<IN> of(PCollection<IN> input) {
    return new BuilderBatch1<>(input);
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
      Windowing<IN, ?, W> windowing,
      ReduceFunction<VALUE, OUT> reducer) {
    super("ReduceByKey", flow, input, keyExtractor, windowing);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.setPartitioning(new HashPartitioning<>(
        input.getPartitioning().getNumPartitions()));
  }

  @SuppressWarnings("unchecked")
  ReduceByKey(Flow flow, Dataset<IN> input,
      UnaryFunction<IN, KEY> keyExtractor,
      UnaryFunction<IN, VALUE> valueExtractor,
      ReduceFunction<VALUE, OUT> reducer) {
    this(flow, input, keyExtractor, valueExtractor,
        (Windowing) BatchWindowing.get(), reducer);
  }

  
  public ReduceFunction<VALUE, OUT> getReducer() {
    return reducer;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TYPE output() {
    return super.output();
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
  public DAG<Operator<?, ?, ?>> getBasicOps() {
    // this can be implemented using ReduceStateByKey

    Flow flow = getFlow();
    ReduceStateByKey<IN, IN, IN, KEY, VALUE, OUT, ReduceState, W, TYPE> reduceState;
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
