
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Reduce by key applied on grouped dataset.
 */
public class GroupReduceByKey<GROUP_KEY, IN, KEY, VALUE, OUT, W extends Window<?, ?>>
    extends StateAwareWindowWiseSingleInputOperator<Pair<GROUP_KEY, IN>, IN,
        Pair<GROUP_KEY, IN>, CompositeKey<GROUP_KEY, KEY>,
        Pair<CompositeKey<GROUP_KEY, KEY>, OUT>, W,
        GroupReduceByKey<GROUP_KEY, IN, KEY, VALUE, OUT, W>>{


  public static class Builder1<GROUP_KEY, IN> {
    final GroupedDataset<GROUP_KEY, IN> input;
    Builder1(GroupedDataset<GROUP_KEY, IN> input) {
      this.input = input;
    }
    public <KEY> Builder2<GROUP_KEY, IN, KEY> keyBy(
        UnaryFunction<IN, KEY> keyExtractor) {
      return new Builder2<>(input, keyExtractor);
    }
    public <VALUE> Builder3<GROUP_KEY, IN, IN, VALUE> valueBy(
        UnaryFunction<IN, VALUE> valueExtractor) {
      return new Builder3<>(input, e -> e, valueExtractor);
    }
    public <OUT> Builder4<GROUP_KEY, IN, IN, IN, OUT> reduceBy(
        ReduceFunction<IN, OUT> reducer) {
      return new Builder4<>(input, e -> e, e-> e, reducer);
    }
    @SuppressWarnings("unchecked")
    public Builder4<GROUP_KEY, IN, IN, IN, IN> combineBy(
        CombinableReduceFunction<IN> reducer) {
      return new Builder4<>(
          input, e -> e, e -> e, (ReduceFunction) reducer);
    }
  }
  public static class Builder2<GROUP_KEY, IN, KEY> {
    final GroupedDataset<GROUP_KEY, IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    Builder2(GroupedDataset<GROUP_KEY, IN> input,
        UnaryFunction<IN, KEY> keyExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
    }
    public <VALUE> Builder3<GROUP_KEY, IN, KEY, VALUE> valueBy(
        UnaryFunction<IN, VALUE> valueExtractor) {
      return new Builder3<>(input, keyExtractor, valueExtractor);
    }
    public <OUT> Builder4<GROUP_KEY, IN, KEY, IN, OUT> reduceBy(
        ReduceFunction<IN, OUT> reducer) {
      return new Builder4<>(input, keyExtractor, e-> e, reducer);
    }
    @SuppressWarnings("unchecked")
    public Builder4<GROUP_KEY, IN, KEY, IN, IN> combineBy(
        CombinableReduceFunction<IN> reducer) {
      return new Builder4<>(
          input, keyExtractor, e -> e, (ReduceFunction) reducer);
    }
  }
  public static class Builder3<GROUP_KEY, IN, KEY, VALUE> {
    final GroupedDataset<GROUP_KEY, IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, VALUE> valueExtractor;
    Builder3(GroupedDataset<GROUP_KEY, IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
    }
    public <OUT> Builder4<GROUP_KEY, IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return new Builder4<>(input, keyExtractor, valueExtractor, reducer);
    }
    public Builder4<GROUP_KEY, IN, KEY, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new Builder4<>(input, keyExtractor, valueExtractor, reducer);
    }
  }
  public static class Builder4<GROUP_KEY, IN, KEY, VALUE, OUT> {
    final GroupedDataset<GROUP_KEY, IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, VALUE> valueExtractor;
    final ReduceFunction<VALUE, OUT> reducer;
    Builder4(GroupedDataset<GROUP_KEY, IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor,
        ReduceFunction<VALUE, OUT> reducer) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.reducer = reducer;
    }
    public <W extends Window<?, ?>> GroupReduceByKey<GROUP_KEY, IN, KEY,
        VALUE, OUT, W>
    windowBy(Windowing<IN, ?, ?, W> windowing) {
      Flow flow = input.getFlow();
      GroupReduceByKey<GROUP_KEY, IN, KEY, VALUE, OUT, W> reduceByKey;
      reduceByKey = new GroupReduceByKey<>(
            flow, input, keyExtractor,
            valueExtractor, windowing, reducer);
      return flow.add(reduceByKey);
    }
    @SuppressWarnings("unchecked")
    public Dataset<Pair<CompositeKey<GROUP_KEY, KEY>, VALUE>> output() {
      return windowBy((Windowing) BatchWindowing.get()).output();
    }

  }


  private final ReduceFunction<VALUE, OUT> reducer;
  private final UnaryFunction<IN, VALUE> valueExtractor;   
  
  GroupReduceByKey(
      Flow flow, Dataset<Pair<GROUP_KEY, IN>> input,
      UnaryFunction<IN, KEY> keyExtractor,
      UnaryFunction<IN, VALUE> valueExtractor,
      Windowing<IN, ?, ?, W> windowing,
      ReduceFunction<VALUE, OUT> reducer) {

    super("GroupReduceByKey", flow, input,
        k -> CompositeKey.of(k.getFirst(), keyExtractor.apply(k.getSecond())),
        windowing);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
  }



  // basic ops implementation

  class ReduceState extends State<VALUE, Pair<CompositeKey<GROUP_KEY, KEY>, OUT>> {

    final List<VALUE> reducableValues = new ArrayList<>();
    final boolean isCombinable = reducer instanceof CombinableReduceFunction;
    CompositeKey<GROUP_KEY, KEY> key;

    ReduceState(CompositeKey<GROUP_KEY, KEY> key,
        Collector<Pair<CompositeKey<GROUP_KEY, KEY>, OUT>> collector) {
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
    // implement this by ReduceStateByKey
    ReduceStateByKey<Pair<GROUP_KEY, IN>, IN, Pair<GROUP_KEY, IN>,
        CompositeKey<GROUP_KEY, KEY>, VALUE,
        OUT, ReduceState, W> reduceState;

    Flow flow = input.getFlow();
    reduceState = new ReduceStateByKey<>(
        flow, (GroupedDataset<GROUP_KEY, IN>) input, keyExtractor,
        k -> valueExtractor.apply(k.getSecond()),
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
