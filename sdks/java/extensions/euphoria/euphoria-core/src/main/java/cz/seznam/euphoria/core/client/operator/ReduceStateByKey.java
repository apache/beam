
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Pair;

/**
 * Operator reducing state by given reduce function.
 */
public class ReduceStateByKey<
    IN, WIN, KIN, KEY, VALUE, OUT, STATE extends State<VALUE, Pair<KEY, OUT>>,
    W extends Window<?>>
    extends StateAwareWindowWiseSingleInputOperator<IN, WIN, KIN, KEY, Pair<KEY, OUT>, W,
        ReduceStateByKey<IN, WIN, KIN, KEY, VALUE, OUT, STATE, W>> {

  public static class Builder1<IN, KIN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <KEY> Builder2<IN, KIN, KEY> keyBy(UnaryFunction<KIN, KEY> keyExtractor) {
      return new Builder2<>(input, keyExtractor);
    }
  }
  public static class Builder2<IN, KIN, KEY> {
    final Dataset<IN> input;
    final UnaryFunction<KIN, KEY> keyExtractor;
    Builder2(Dataset<IN> input, UnaryFunction<KIN, KEY> keyExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
    }
    public <VALUE> Builder3<IN, KIN, KEY, VALUE> valueBy(UnaryFunction<KIN, VALUE> valueExtractor) {
      return new Builder3<>(input, keyExtractor, valueExtractor);
    }
  }
  public static class Builder3<IN, KIN, KEY, VALUE> {
    final Dataset<IN> input;
    final UnaryFunction<KIN, KEY> keyExtractor;
    final UnaryFunction<KIN, VALUE> valueExtractor;
    Builder3(Dataset<IN> input,
        UnaryFunction<KIN, KEY> keyExtractor,
        UnaryFunction<KIN, VALUE> valueExtractor) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
    }
    public <OUT, STATE extends State<VALUE, Pair<KEY, OUT>>> Builder4<
    IN, KIN, KEY, VALUE, OUT, STATE> stateFactory(
        BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory) {
      return new Builder4<>(input, keyExtractor, valueExtractor, stateFactory);
    }
  }
  public static class Builder4<
      IN, KIN, KEY, VALUE, OUT, STATE extends State<VALUE, Pair<KEY, OUT>>> {
    final Dataset<IN> input;
    final UnaryFunction<KIN, KEY> keyExtractor;
    final UnaryFunction<KIN, VALUE> valueExtractor;
    final BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory;
    Builder4(Dataset<IN> input,
        UnaryFunction<KIN, KEY> keyExtractor,
        UnaryFunction<KIN, VALUE> valueExtractor,
        BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.stateFactory = stateFactory;
    }
    public Builder5<IN, KIN, KEY, VALUE, OUT, STATE> combineStateBy(
        CombinableReduceFunction<STATE> stateCombiner) {
      return new Builder5<>(input, keyExtractor, valueExtractor, stateFactory,
          stateCombiner);
    }
  }
  public static class Builder5<
      IN, KIN, KEY, VALUE, OUT, STATE extends State<VALUE, Pair<KEY, OUT>>> {
    final Dataset<IN> input;
    final UnaryFunction<KIN, KEY> keyExtractor;
    final UnaryFunction<KIN, VALUE> valueExtractor;
    final BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory;
    final CombinableReduceFunction<STATE> stateCombiner;
    Builder5(Dataset<IN> input,
        UnaryFunction<KIN, KEY> keyExtractor,
        UnaryFunction<KIN, VALUE> valueExtractor,
        BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory,
        CombinableReduceFunction<STATE> stateCombiner) {
      this.input = input;
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.stateFactory = stateFactory;
      this.stateCombiner = stateCombiner;
    }
    public <WIN, W extends Window<?>> ReduceStateByKey<
        IN, WIN, KIN, KEY, VALUE, OUT, STATE, W> windowBy(
        Windowing<WIN, ?, W> windowing) {
      Flow flow = input.getFlow();
      return flow.add(new ReduceStateByKey<>(
          flow, input, keyExtractor, valueExtractor,
          windowing, stateFactory, stateCombiner));
    }
    public Dataset<Pair<KEY, OUT>> output() {
      return windowBy(BatchWindowing.get()).output();
    }

  }


  public static <IN> Builder1<IN, IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  private final BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory;
  private final UnaryFunction<KIN, VALUE> valueExtractor;
  private final CombinableReduceFunction<STATE> stateCombiner;
  private final boolean grouped;
  
  ReduceStateByKey(Flow flow,
      Dataset<IN> input,
      UnaryFunction<KIN, KEY> keyExtractor,
      UnaryFunction<KIN, VALUE> valueExtractor,
      Windowing<WIN, ?, W> windowing,
      BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory,
      CombinableReduceFunction<STATE> stateCombiner) {
    super("ReduceStateByKey", flow, input, keyExtractor, windowing);
    this.stateFactory = stateFactory;
    this.valueExtractor = valueExtractor;
    this.stateCombiner = stateCombiner;
    this.setPartitioning(
        new HashPartitioning<>(input.getPartitioning().getNumPartitions()));
    this.grouped = false;
  }



  @SuppressWarnings("unchecked")
  ReduceStateByKey(Flow flow,
      GroupedDataset<Object, IN> groupedInput,
      UnaryFunction<KIN, KEY> keyExtractor,
      UnaryFunction<KIN, VALUE> valueExtractor,
      Windowing<WIN, ?, W> windowing,
      BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> stateFactory,
      CombinableReduceFunction<STATE> stateCombiner) {
    super("ReduceStateByKey", flow, (Dataset) groupedInput, keyExtractor, windowing);
    this.stateFactory = stateFactory;
    this.valueExtractor = valueExtractor;
    this.stateCombiner = stateCombiner;
    this.setPartitioning(
        new HashPartitioning<>(input.getPartitioning().getNumPartitions()));
    this.grouped = true;
  }


  public BinaryFunction<KEY, Collector<Pair<KEY, OUT>>, STATE> getStateFactory() {
    return stateFactory;
  }

  public UnaryFunction<KIN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  public CombinableReduceFunction<STATE> getStateCombiner() {
    return stateCombiner;
  }

  public boolean isGrouped() {
    return grouped;
  }



}
