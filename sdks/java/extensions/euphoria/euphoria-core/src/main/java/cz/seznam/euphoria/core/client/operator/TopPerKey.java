package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

@Derived(
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class TopPerKey<
        IN, KEY, VALUE, SCORE extends Comparable<SCORE>, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, KEY, Triple<KEY, VALUE, SCORE>, W,
    TopPerKey<IN, KEY, VALUE, SCORE, W>> {
  
  private static final class MaxScored<V, C extends Comparable<C>>
      extends State<Pair<V, C>, Pair<V, C>> {
    
    final ValueStorage<Pair<V, C>> curr;

    @SuppressWarnings("unchecked")
    MaxScored(
        Context<Pair<V, C>> context,
        StorageProvider storageProvider) {

      super(context, storageProvider);
      curr = (ValueStorage) storageProvider.getValueStorage(
          ValueStorageDescriptor.of("max", Pair.class, Pair.of(null, null)));
    }

    void merge(MaxScored<V, C> other) {
      if (other.curr.get().getFirst() != null) {
        this.add(other.curr.get());
      }
    }

    @Override
    public void add(Pair<V, C> element) {
      if (curr.get().getFirst() == null
          || element.getSecond().compareTo(curr.get().getSecond()) > 0) {
        curr.set(element);
      }
    }

    @Override
    public void flush() {
      if (curr.get().getFirst() != null) {
        getContext().collect(curr.get());
      }
    }

    @Override
    public void close() {
      curr.clear();
    }


  }

  public static class KeyByBuilder<IN> {
    private final Dataset<IN> input;

    KeyByBuilder(Dataset<IN> input) {
      this.input = input;
    }

    public <K> ValueByBuilder<IN, K> keyBy(UnaryFunction<IN, K> keyFn) {
      return new ValueByBuilder<>(input, requireNonNull(keyFn));
    }
  }

  public static class ValueByBuilder<IN, K> {
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;

    ValueByBuilder(Dataset<IN> input, UnaryFunction<IN, K> keyFn) {
      this.input = input;
      this.keyFn = keyFn;
    }

    public <V> ScoreByBuilder<IN, K, V> valueBy(UnaryFunction<IN, V> valueFn) {
      return new ScoreByBuilder<>(input, keyFn, requireNonNull(valueFn));
    }
  }

  public static class ScoreByBuilder<IN, K, V> {
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;

    ScoreByBuilder(Dataset<IN> input,
                   UnaryFunction<IN, K> keyFn,
                   UnaryFunction<IN, V> valueFn)
    {
      this.input = input;
      this.keyFn = keyFn;
      this.valueFn = valueFn;
    }

    public <S extends Comparable<S>> WindowByBuilder<IN, K, V, S>
    scoreBy(UnaryFunction<IN, S> scoreFn)
    {
      return new WindowByBuilder<>(input, keyFn, valueFn, requireNonNull(scoreFn));
    }
  }

  public static class WindowByBuilder<IN, K, V, S extends Comparable<S>>
      extends PartitioningBuilder<IN, WindowByBuilder<IN, K, V, S>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Triple<K, V, S>>
  {
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;
    private final UnaryFunction<IN, S> scoreFn;

    WindowByBuilder(Dataset<IN> input,
                    UnaryFunction<IN, K> keyFn,
                    UnaryFunction<IN, V> valueFn,
                    UnaryFunction<IN, S> scoreFn)
    {
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.input = input;
      this.keyFn = keyFn;
      this.valueFn = valueFn;
      this.scoreFn = scoreFn;
    }

    public <W extends Window>
    OutputBuilder<IN, K, V, S, W>
    windowBy(Windowing<IN, W> windowing)
    {
      return new OutputBuilder<>(input, keyFn, valueFn, scoreFn, requireNonNull(windowing));
    }

    @Override
    public Dataset<Triple<K, V, S>> output() {
      return new OutputBuilder<>(input, keyFn, valueFn, scoreFn, null).output();
    }
  }

  public static class OutputBuilder<
      IN, K, V, S extends Comparable<S>, W extends Window>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Triple<K, V, S>>
  {
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;
    private final UnaryFunction<IN, S> scoreFn;
    private final Windowing<IN, W> windowing;

    OutputBuilder(Dataset<IN> input, UnaryFunction<IN, K> keyFn,
                  UnaryFunction<IN, V> valueFn, UnaryFunction<IN, S> scoreFn,
                  Windowing<IN, W> windowing)
    {
      this.input = input;
      this.keyFn = keyFn;
      this.valueFn = valueFn;
      this.scoreFn = scoreFn;
      this.windowing = windowing;
    }

    @Override
    public Dataset<Triple<K, V, S>> output() {
      Flow flow = input.getFlow();
      TopPerKey<IN, K, V, S, W> top =
          new TopPerKey<>(flow, input, keyFn, valueFn, scoreFn, windowing);
      flow.add(top);
      return top.output();
    }
  }

  public static <I> KeyByBuilder<I> of(Dataset<I> input) {
    return new KeyByBuilder<>(requireNonNull(input));
  }

  // ~ -----------------------------------------------------------------------------

  private final UnaryFunction<IN, VALUE> valueFn;
  private final UnaryFunction<IN, SCORE> scoreFn;

  TopPerKey(Flow flow, Dataset<IN> input,
            UnaryFunction<IN, KEY> keyFn,
            UnaryFunction<IN, VALUE> valueFn,
            UnaryFunction<IN, SCORE> scoreFn,
            Windowing<IN, W> windowing)
  {
    super("Top", flow, input, keyFn, windowing);
    this.valueFn = valueFn;
    this.scoreFn = scoreFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    ReduceStateByKey<IN, IN, IN, KEY, Pair<VALUE, SCORE>, KEY, Pair<VALUE, SCORE>,
        MaxScored<VALUE, SCORE>, W>
        reduce =
        new ReduceStateByKey<>(getName() + "::ReduceStateByKey", flow, input,
            keyExtractor,
            e -> Pair.of(valueFn.apply(e), scoreFn.apply(e)),
            windowing,
            MaxScored::new,
            (CombinableReduceFunction<MaxScored<VALUE, SCORE>>) states -> {
              Iterator<MaxScored<VALUE, SCORE>> iter = states.iterator();
              MaxScored<VALUE, SCORE> m = iter.next();
              while (iter.hasNext()) {
                m.merge(iter.next());
              }
              return m;
            },
            partitioning);

    MapElements<Pair<KEY, Pair<VALUE, SCORE>>, Triple<KEY, VALUE, SCORE>>
        format =
        new MapElements<>(getName() + "::MapElements", flow, reduce.output(),
            e -> Triple.of(
                e.getFirst(),
                e.getSecond().getFirst(),
                e.getSecond().getSecond()));

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);

    return dag;
  }
}
