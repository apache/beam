/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Emits top element for defined keys and windows. The elements are compared by comparable
 * objects extracted by user defined function applied on input elements.
 *
 * Custom {@link Windowing} can be set, otherwise values from
 * input operator are used.<p>
 *
 * Example:
 *
 * <pre>{@code
 *  TopPerKey.of(elements)
 *       .keyBy(e -> (byte) 0)
 *       .valueBy(e -> e)
 *       .scoreBy(Pair::getSecond)
 *       .output();
 * }</pre>
 *
 * The examples above finds global maximum of all elements.
 */
@Audience(Audience.Type.CLIENT)
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
      implements State<Pair<V, C>, Pair<V, C>>, StateSupport.MergeFrom<MaxScored<V, C>> {

    static final ValueStorageDescriptor<Pair> MAX_STATE_DESCR =
            ValueStorageDescriptor.of("max", Pair.class, Pair.of(null, null));

    final ValueStorage<Pair<V, C>> curr;

    @SuppressWarnings("unchecked")
    MaxScored(StorageProvider storageProvider) {
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
    public void flush(Collector<Pair<V, C>> context) {
      Pair<V, C> c = curr.get();
      if (c.getFirst() != null) {
        context.collect(c);
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

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  public static class KeyByBuilder<IN> implements Builders.KeyBy<IN> {
    private final String name;
    private final Dataset<IN> input;

    KeyByBuilder(String name, Dataset<IN> input) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
    }

    @Override
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
      implements Builders.WindowBy<IN>, Builders.Output<Triple<K, V, S>>
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
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
      this.scoreFn = requireNonNull(scoreFn);
    }

    @Override
    public <W extends Window>
    OutputBuilder<IN, K, V, S, W>
    windowBy(Windowing<IN, W> windowing) {
      return new OutputBuilder<>(name, input, keyFn, valueFn,
          scoreFn, requireNonNull(windowing));
    }

    @Override
    public Dataset<Triple<K, V, S>> output() {
      return new OutputBuilder<>(
          name, input, keyFn, valueFn, scoreFn, null).output();
    }
  }

  public static class OutputBuilder<
      IN, K, V, S extends Comparable<S>, W extends Window>
      implements Builders.Output<Triple<K, V, S>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, K> keyFn;
    private final UnaryFunction<IN, V> valueFn;
    private final UnaryFunction<IN, S> scoreFn;
    @Nullable
    private final Windowing<IN, W> windowing;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, K> keyFn,
                  UnaryFunction<IN, V> valueFn,
                  UnaryFunction<IN, S> scoreFn,
                  @Nullable Windowing<IN, W> windowing) {

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
      this.scoreFn = requireNonNull(scoreFn);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Triple<K, V, S>> output() {
      Flow flow = input.getFlow();
      TopPerKey<IN, K, V, S, W> top =
          new TopPerKey<>(flow, name, input, keyFn, valueFn,
                  scoreFn, windowing);
      flow.add(top);
      return top.output();
    }
  }

  /**
   * Starts building a nameless {@link TopPerKey} operator to process
   * the given input dataset.
   *
   * @param <IN> the type of elements of the input dataset
   *
   * @param input the input data set to be processed
   *
   * @return a builder to complete the setup of the new operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
    return new KeyByBuilder<>("TopPerKey", input);
  }

  /**
   * Starts building a named {@link TopPerKey} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
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
            @Nullable Windowing<IN, W> windowing) {
    super(name, flow, input, keyFn, windowing);

    this.valueFn = valueFn;
    this.scoreFn = scoreFn;
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

    StateSupport.MergeFromStateMerger<Pair<VALUE, SCORE>, Pair<VALUE, SCORE>, MaxScored<VALUE, SCORE>>
            stateCombiner = new StateSupport.MergeFromStateMerger<>();
    ReduceStateByKey<IN, KEY, Pair<VALUE, SCORE>, Pair<VALUE, SCORE>,
        MaxScored<VALUE, SCORE>, W>
        reduce =
        new ReduceStateByKey<>(getName() + "::ReduceStateByKey", flow, input,
                keyExtractor,
                e -> Pair.of(valueFn.apply(e), scoreFn.apply(e)),
                windowing,
                (StorageProvider storageProvider, Collector<Pair<VALUE, SCORE>> ctx) -> new MaxScored<>(storageProvider),
                stateCombiner);

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
