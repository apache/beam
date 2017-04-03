/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;

import javax.annotation.Nullable;

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
      extends State<Pair<V, C>, Pair<V, C>>
      implements StateSupport.MergeFrom<MaxScored<V, C>> {

    static final ValueStorageDescriptor<Pair> MAX_STATE_DESCR =
            ValueStorageDescriptor.of("max", Pair.class, Pair.of(null, null));

    final ValueStorage<Pair<V, C>> curr;

    @SuppressWarnings("unchecked")
    MaxScored(Context<Pair<V, C>> context, StorageProvider storageProvider) {
      super(context);
      curr = (ValueStorage) storageProvider.getValueStorage(MAX_STATE_DESCR);
    }

    @Override
    public void add(Pair<V, C> element) {
      Pair<V, C> c = curr.get();
      if (c.getFirst() == null || element.getSecond().compareTo(c.getSecond()) > 0) {
        curr.set(element);
      }
    }

    @Override
    public void flush() {
      Pair<V, C> c = curr.get();
      if (c.getFirst() != null) {
        getContext().collect(c);
      }
    }

    @Override
    public void close() {
      curr.clear();
    }

    @Override
    public void mergeFrom(MaxScored<V, C> other) {
      Pair<V, C> o = other.curr.get();
      if (o.getFirst() != null) {
        this.add(o);
      }
    }
  }

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  public static class KeyByBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    KeyByBuilder(String name, Dataset<IN> input) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
    }

    public <K> ValueByBuilder<IN, K> keyBy(UnaryFunction<IN, K> keyFn) {
      return new ValueByBuilder<>(name, input, requireNonNull(keyFn));
    }
  }

  public static class ValueByBuilder<IN, K> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;

    ValueByBuilder(String name, Dataset<IN> input, UnaryFunction<IN, K> keyFn) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
    }

    public <V> ScoreByBuilder<IN, K, V> valueBy(UnaryFunction<IN, V> valueFn) {
      return new ScoreByBuilder<>(name, input, keyFn, requireNonNull(valueFn));
    }
  }

  public static class ScoreByBuilder<IN, K, V> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;

    ScoreByBuilder(String name,
                   Dataset<IN> input,
                   UnaryFunction<IN, K> keyFn,
                   UnaryFunction<IN, V> valueFn) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
    }

    public <S extends Comparable<S>> WindowByBuilder<IN, K, V, S>
    scoreBy(UnaryFunction<IN, S> scoreFn)
    {
      return new WindowByBuilder<>(name, input, keyFn, valueFn, requireNonNull(scoreFn));
    }
  }

  public static class WindowByBuilder<IN, K, V, S extends Comparable<S>>
      extends PartitioningBuilder<K, WindowByBuilder<IN, K, V, S>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Triple<K, V, S>>
  {
    private String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;
    private final UnaryFunction<IN, S> scoreFn;

    WindowByBuilder(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, K> keyFn,
                    UnaryFunction<IN, V> valueFn,
                    UnaryFunction<IN, S> scoreFn)
    {
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
      this.scoreFn = requireNonNull(scoreFn);
    }

    public <W extends Window>
    OutputBuilder<IN, K, V, S, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }

    public <W extends Window>
    OutputBuilder<IN, K, V, S, W>
    windowBy(Windowing<IN, W> windowing, ExtractEventTime<IN> eventTimeAssigner) {
      return new OutputBuilder<>(name, input, keyFn, valueFn,
              scoreFn, this, requireNonNull(windowing), eventTimeAssigner);
    }

    @Override
    public Dataset<Triple<K, V, S>> output() {
      return new OutputBuilder<>(
          name, input, keyFn, valueFn, scoreFn, this, null, null).output();
    }
  }

  public static class OutputBuilder<
      IN, K, V, S extends Comparable<S>, W extends Window>
      extends PartitioningBuilder<K, OutputBuilder<IN, K, V, S, W>>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<Triple<K, V, S>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;
    private final UnaryFunction<IN, S> scoreFn;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final ExtractEventTime<IN> eventTimeAssigner;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, K> keyFn,
                  UnaryFunction<IN, V> valueFn,
                  UnaryFunction<IN, S> scoreFn,
                  PartitioningBuilder<K, ?> partitioning,
                  @Nullable Windowing<IN, W> windowing,
                  @Nullable ExtractEventTime<IN> eventTimeAssigner) {

      super(partitioning);

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
      this.scoreFn = requireNonNull(scoreFn);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<Triple<K, V, S>> output() {
      Flow flow = input.getFlow();
      TopPerKey<IN, K, V, S, W> top =
          new TopPerKey<>(flow, name, input, keyFn, valueFn,
                  scoreFn, getPartitioning(), windowing, eventTimeAssigner);
      flow.add(top);
      return top.output();
    }
  }

  public static <I> KeyByBuilder<I> of(Dataset<I> input) {
    return new KeyByBuilder<>("TopPerKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  // ~ -----------------------------------------------------------------------------

  private final UnaryFunction<IN, VALUE> valueFn;
  private final UnaryFunction<IN, SCORE> scoreFn;

  TopPerKey(Flow flow,
            String name,
            Dataset<IN> input,
            UnaryFunction<IN, KEY> keyFn,
            UnaryFunction<IN, VALUE> valueFn,
            UnaryFunction<IN, SCORE> scoreFn,
            Partitioning<KEY> partitioning,
            @Nullable Windowing<IN, W> windowing,
            @Nullable ExtractEventTime<IN> eventTimeAssigner) {
    super(name, flow, input, keyFn, windowing, eventTimeAssigner, partitioning);
    
    this.valueFn = valueFn;
    this.scoreFn = scoreFn;
    this.partitioning = partitioning;
  }

  public UnaryFunction<IN, VALUE> getValueExtractor() {
    return valueFn;
  }

  public UnaryFunction<IN, SCORE> getScoreExtractor() {
    return scoreFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    StateSupport.MergeFromStateCombiner<MaxScored<VALUE, SCORE>> stateCombiner
            = new StateSupport.MergeFromStateCombiner<>();
    ReduceStateByKey<IN, IN, IN, KEY, Pair<VALUE, SCORE>, KEY, Pair<VALUE, SCORE>,
        MaxScored<VALUE, SCORE>, W>
        reduce =
        new ReduceStateByKey<>(getName() + "::ReduceStateByKey", flow, input,
                keyExtractor,
                e -> Pair.of(valueFn.apply(e), scoreFn.apply(e)),
                windowing,
                eventTimeAssigner,
                (StateFactory<Pair<VALUE, SCORE>, Pair<VALUE, SCORE>, MaxScored<VALUE, SCORE>>) MaxScored::new,
                stateCombiner,
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
