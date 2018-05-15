/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;

/**
 * Emits top element for defined keys and windows. The elements are compared by comparable objects
 * extracted by user defined function applied on input elements.
 *
 * <p>Custom {@link Windowing} can be set, otherwise values from input operator are used.
 *
 * <p>Example:
 *
 * <pre>{@code
 * TopPerKey.of(elements)
 *      .keyBy(e -> (byte) 0)
 *      .valueBy(e -> e)
 *      .scoreBy(Pair::getSecond)
 *      .output();
 * }</pre>
 *
 * <p>The examples above finds global maximum of all elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code valueBy ..................} value extractor function
 *   <li>{@code scoreBy ..................} {@link UnaryFunction} transforming input elements to
 *       {@link Comparable} scores
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class TopPerKey<InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends Window<W>>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, K, Triple<K, V, ScoreT>, W, TopPerKey<InputT, K, V, ScoreT, W>> {

  private final UnaryFunction<InputT, V> valueFn;
  private final UnaryFunction<InputT, ScoreT> scoreFn;

  TopPerKey(
      Flow flow,
      String name,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyFn,
      UnaryFunction<InputT, V> valueFn,
      UnaryFunction<InputT, ScoreT> scoreFn,
      @Nullable Windowing<InputT, W> windowing,
      Set<OutputHint> outputHints) {
    super(name, flow, input, keyFn, windowing, outputHints);

    this.valueFn = valueFn;
    this.scoreFn = scoreFn;
  }

  /**
   * Starts building a nameless {@link TopPerKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("TopPerKey", input);
  }

  /**
   * Starts building a named {@link TopPerKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public UnaryFunction<InputT, V> getValueExtractor() {
    return valueFn;
  }

  public UnaryFunction<InputT, ScoreT> getScoreExtractor() {
    return scoreFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = getFlow();

    StateSupport.MergeFromStateMerger<Pair<V, ScoreT>, Pair<V, ScoreT>, MaxScored<V, ScoreT>>
        stateCombiner = new StateSupport.MergeFromStateMerger<>();
    ReduceStateByKey<InputT, K, Pair<V, ScoreT>, Pair<V, ScoreT>, MaxScored<V, ScoreT>, W> reduce =
        new ReduceStateByKey<>(
            getName() + "::ReduceStateByKey",
            flow,
            input,
            keyExtractor,
            e -> Pair.of(valueFn.apply(e), scoreFn.apply(e)),
            windowing,
            (StateContext context, Collector<Pair<V, ScoreT>> collector) -> {
              return new MaxScored<>(context.getStorageProvider());
            },
            stateCombiner,
            Collections.emptySet());

    MapElements<Pair<K, Pair<V, ScoreT>>, Triple<K, V, ScoreT>> format =
        new MapElements<>(
            getName() + "::MapElements",
            flow,
            reduce.output(),
            e -> Triple.of(e.getFirst(), e.getSecond().getFirst(), e.getSecond().getSecond()),
            getHints());

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);

    return dag;
  }

  /** TODO: complete javadoc. */
  private static final class MaxScored<V, CompareT extends Comparable<CompareT>>
      implements State<Pair<V, CompareT>, Pair<V, CompareT>>,
          StateSupport.MergeFrom<MaxScored<V, CompareT>> {

    static final ValueStorageDescriptor<Pair> MAX_STATE_DESCR =
        ValueStorageDescriptor.of("max", Pair.class, Pair.of(null, null));

    final ValueStorage<Pair<V, CompareT>> curr;

    @SuppressWarnings("unchecked")
    MaxScored(StorageProvider storageProvider) {
      curr = (ValueStorage) storageProvider.getValueStorage(MAX_STATE_DESCR);
    }

    @Override
    public void add(Pair<V, CompareT> element) {
      Pair<V, CompareT> c = curr.get();
      if (c.getFirst() == null || element.getSecond().compareTo(c.getSecond()) > 0) {
        curr.set(element);
      }
    }

    @Override
    public void flush(Collector<Pair<V, CompareT>> context) {
      Pair<V, CompareT> c = curr.get();
      if (c.getFirst() != null) {
        context.collect(c);
      }
    }

    @Override
    public void close() {
      curr.clear();
    }

    @Override
    public void mergeFrom(MaxScored<V, CompareT> other) {
      Pair<V, CompareT> o = other.curr.get();
      if (o.getFirst() != null) {
        this.add(o);
      }
    }
  }

  // ~ -----------------------------------------------------------------------------

  /** TODO: complete javadoc. */
  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {
    private final String name;
    private final Dataset<InputT> input;

    KeyByBuilder(String name, Dataset<InputT> input) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyFn) {
      return new ValueByBuilder<>(name, input, requireNonNull(keyFn));
    }
  }

  /** TODO: complete javadoc. */
  public static class ValueByBuilder<InputT, K> {
    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyFn;

    ValueByBuilder(String name, Dataset<InputT> input, UnaryFunction<InputT, K> keyFn) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
    }

    public <V> ScoreByBuilder<InputT, K, V> valueBy(UnaryFunction<InputT, V> valueFn) {
      return new ScoreByBuilder<>(name, input, keyFn, requireNonNull(valueFn));
    }
  }

  /** TODO: complete javadoc. */
  public static class ScoreByBuilder<InputT, K, V> {
    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyFn;
    private final UnaryFunction<InputT, V> valueFn;

    ScoreByBuilder(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyFn,
        UnaryFunction<InputT, V> valueFn) {
      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
    }

    public <ScoreT extends Comparable<ScoreT>> WindowByBuilder<InputT, K, V, ScoreT> scoreBy(
        UnaryFunction<InputT, ScoreT> scoreFn) {
      return new WindowByBuilder<>(name, input, keyFn, valueFn, requireNonNull(scoreFn));
    }
  }

  /** TODO: complete javadoc. */
  public static class WindowByBuilder<InputT, K, V, ScoreT extends Comparable<ScoreT>>
      implements Builders.WindowBy<InputT, WindowByBuilder<InputT, K, V, ScoreT>>,
          Builders.Output<Triple<K, V, ScoreT>> {
    final String name;
    final Dataset<InputT> input;
    final UnaryFunction<InputT, K> keyFn;
    final UnaryFunction<InputT, V> valueFn;
    final UnaryFunction<InputT, ScoreT> scoreFn;

    WindowByBuilder(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyFn,
        UnaryFunction<InputT, V> valueFn,
        UnaryFunction<InputT, ScoreT> scoreFn) {

      this.name = requireNonNull(name);
      this.input = requireNonNull(input);
      this.keyFn = requireNonNull(keyFn);
      this.valueFn = requireNonNull(valueFn);
      this.scoreFn = requireNonNull(scoreFn);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, V, ScoreT, W> windowBy(
        Windowing<InputT, W> windowing) {
      return new OutputBuilder<>(name, input, keyFn, valueFn, scoreFn, requireNonNull(windowing));
    }

    @Override
    public Dataset<Triple<K, V, ScoreT>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(name, input, keyFn, valueFn, scoreFn, null).output(outputHints);
    }
  }

  /** TODO: complete javadoc. */
  public static class OutputBuilder<
          InputT, K, V, ScoreT extends Comparable<ScoreT>, W extends Window<W>>
      extends WindowByBuilder<InputT, K, V, ScoreT> {

    @Nullable private final Windowing<InputT, W> windowing;

    OutputBuilder(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyFn,
        UnaryFunction<InputT, V> valueFn,
        UnaryFunction<InputT, ScoreT> scoreFn,
        @Nullable Windowing<InputT, W> windowing) {

      super(name, input, keyFn, valueFn, scoreFn);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Triple<K, V, ScoreT>> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      TopPerKey<InputT, K, V, ScoreT, W> top =
          new TopPerKey<>(
              flow, name, input, keyFn, valueFn, scoreFn, windowing, Sets.newHashSet(outputHints));
      flow.add(top);
      return top.output();
    }
  }
}
