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

package org.apache.beam.runners.spark.translation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

/** A set of utilities to help translating Beam transformations into Spark transformations. */
public final class TranslationUtils {

  private TranslationUtils() {}

  /**
   * In-memory state internals factory.
   *
   * @param <K> State key type.
   */
  static class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K>, Serializable {
    @Override
    public StateInternals stateInternalsForKey(K key) {
      return InMemoryStateInternals.forKey(key);
    }
  }

  /**
   * A SparkKeyedCombineFn function applied to grouped KVs.
   *
   * @param <K> Grouped key type.
   * @param <InputT> Grouped values type.
   * @param <OutputT> Output type.
   */
  public static class CombineGroupedValues<K, InputT, OutputT>
      implements Function<WindowedValue<KV<K, Iterable<InputT>>>, WindowedValue<KV<K, OutputT>>> {
    private final SparkKeyedCombineFn<K, InputT, ?, OutputT> fn;

    public CombineGroupedValues(SparkKeyedCombineFn<K, InputT, ?, OutputT> fn) {
      this.fn = fn;
    }

    @Override
    public WindowedValue<KV<K, OutputT>> call(WindowedValue<KV<K, Iterable<InputT>>> windowedKv)
        throws Exception {
      return WindowedValue.of(
          KV.of(windowedKv.getValue().getKey(), fn.apply(windowedKv)),
          windowedKv.getTimestamp(),
          windowedKv.getWindows(),
          windowedKv.getPane());
    }
  }

  /**
   * Checks if the window transformation should be applied or skipped.
   *
   * <p>Avoid running assign windows if both source and destination are global window or if the user
   * has not specified the WindowFn (meaning they are just messing with triggering or allowed
   * lateness).
   *
   * @param transform The {@link Window.Assign} transformation.
   * @param context The {@link EvaluationContext}.
   * @param <T> PCollection type.
   * @param <W> {@link BoundedWindow} type.
   * @return if to apply the transformation.
   */
  public static <T, W extends BoundedWindow> boolean skipAssignWindows(
      Window.Assign<T> transform, EvaluationContext context) {
    @SuppressWarnings("unchecked")
    WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();
    return windowFn == null
        || (context.getInput(transform).getWindowingStrategy().getWindowFn()
                instanceof GlobalWindows
            && windowFn instanceof GlobalWindows);
  }

  /** Transform a pair stream into a value stream. */
  public static <T1, T2> JavaDStream<T2> dStreamValues(JavaPairDStream<T1, T2> pairDStream) {
    return pairDStream.map(
        new Function<Tuple2<T1, T2>, T2>() {
          @Override
          public T2 call(Tuple2<T1, T2> v1) throws Exception {
            return v1._2();
          }
        });
  }

  /** {@link KV} to pair function. */
  public static <K, V> PairFunction<KV<K, V>, K, V> toPairFunction() {
    return new PairFunction<KV<K, V>, K, V>() {
      @Override
      public Tuple2<K, V> call(KV<K, V> kv) {
        return new Tuple2<>(kv.getKey(), kv.getValue());
      }
    };
  }

  /** {@link KV} to pair flatmap function. */
  public static <K, V> PairFlatMapFunction<Iterator<KV<K, V>>, K, V> toPairFlatMapFunction() {
    return new PairFlatMapFunction<Iterator<KV<K, V>>, K, V>() {
      @Override
      public Iterable<Tuple2<K, V>> call(final Iterator<KV<K, V>> itr) {
        final Iterator<Tuple2<K, V>> outputItr =
            Iterators.transform(
                itr,
                new com.google.common.base.Function<KV<K, V>, Tuple2<K, V>>() {

                  @Override
                  public Tuple2<K, V> apply(KV<K, V> kv) {
                    return new Tuple2<>(kv.getKey(), kv.getValue());
                  }
                });
        return new Iterable<Tuple2<K, V>>() {

          @Override
          public Iterator<Tuple2<K, V>> iterator() {
            return outputItr;
          }
        };
      }
    };
  }

  /** A pair to {@link KV} function . */
  static <K, V> Function<Tuple2<K, V>, KV<K, V>> fromPairFunction() {
    return new Function<Tuple2<K, V>, KV<K, V>>() {
      @Override
      public KV<K, V> call(Tuple2<K, V> t2) {
        return KV.of(t2._1(), t2._2());
      }
    };
  }

  /** A pair to {@link KV} flatmap function . */
  static <K, V> FlatMapFunction<Iterator<Tuple2<K, V>>, KV<K, V>> fromPairFlatMapFunction() {
    return new FlatMapFunction<Iterator<Tuple2<K, V>>, KV<K, V>>() {
      @Override
      public Iterable<KV<K, V>> call(Iterator<Tuple2<K, V>> itr) {
        final Iterator<KV<K, V>> outputItr =
            Iterators.transform(
                itr,
                new com.google.common.base.Function<Tuple2<K, V>, KV<K, V>>() {
                  @Override
                  public KV<K, V> apply(Tuple2<K, V> t2) {
                    return KV.of(t2._1(), t2._2());
                  }
                });
        return new Iterable<KV<K, V>>() {
          @Override
          public Iterator<KV<K, V>> iterator() {
            return outputItr;
          }
        };
      }
    };
  }

  /** Extract key from a {@link WindowedValue} {@link KV} into a pair. */
  public static <K, V>
      PairFunction<WindowedValue<KV<K, V>>, K, WindowedValue<KV<K, V>>>
          toPairByKeyInWindowedValue() {
    return new PairFunction<WindowedValue<KV<K, V>>, K, WindowedValue<KV<K, V>>>() {
      @Override
      public Tuple2<K, WindowedValue<KV<K, V>>> call(WindowedValue<KV<K, V>> windowedKv)
          throws Exception {
        return new Tuple2<>(windowedKv.getValue().getKey(), windowedKv);
      }
    };
  }

  /** Extract window from a {@link KV} with {@link WindowedValue} value. */
  static <K, V> Function<KV<K, WindowedValue<V>>, WindowedValue<KV<K, V>>> toKVByWindowInValue() {
    return new Function<KV<K, WindowedValue<V>>, WindowedValue<KV<K, V>>>() {
      @Override
      public WindowedValue<KV<K, V>> call(KV<K, WindowedValue<V>> kv) throws Exception {
        WindowedValue<V> wv = kv.getValue();
        return wv.withValue(KV.of(kv.getKey(), wv.getValue()));
      }
    };
  }

  /**
   * A utility class to filter {@link TupleTag}s.
   *
   * @param <V> TupleTag type.
   */
  public static final class TupleTagFilter<V>
      implements Function<Tuple2<TupleTag<V>, WindowedValue<?>>, Boolean> {

    private final TupleTag<V> tag;

    public TupleTagFilter(TupleTag<V> tag) {
      this.tag = tag;
    }

    @Override
    public Boolean call(Tuple2<TupleTag<V>, WindowedValue<?>> input) {
      return tag.equals(input._1());
    }
  }

  /**
   * Create SideInputs as Broadcast variables.
   *
   * @param views The {@link PCollectionView}s.
   * @param context The {@link EvaluationContext}.
   * @return a map of tagged {@link SideInputBroadcast}s and their {@link WindowingStrategy}.
   */
  static Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> getSideInputs(
      List<PCollectionView<?>> views, EvaluationContext context) {
    return getSideInputs(views, context.getSparkContext(), context.getPViews());
  }

  /**
   * Create SideInputs as Broadcast variables.
   *
   * @param views The {@link PCollectionView}s.
   * @param context The {@link JavaSparkContext}.
   * @param pviews The {@link SparkPCollectionView}.
   * @return a map of tagged {@link SideInputBroadcast}s and their {@link WindowingStrategy}.
   */
  public static Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> getSideInputs(
      List<PCollectionView<?>> views, JavaSparkContext context, SparkPCollectionView pviews) {
    if (views == null) {
      return ImmutableMap.of();
    } else {
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs =
          Maps.newHashMap();
      for (PCollectionView<?> view : views) {
        SideInputBroadcast helper = pviews.getPCollectionView(view, context);
        WindowingStrategy<?, ?> windowingStrategy = view.getWindowingStrategyInternal();
        sideInputs.put(
            view.getTagInternal(),
            KV.<WindowingStrategy<?, ?>, SideInputBroadcast<?>>of(windowingStrategy, helper));
      }
      return sideInputs;
    }
  }

  public static void rejectSplittable(DoFn<?, ?> doFn) {
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    if (signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not support splittable DoFn: %s", SparkRunner.class.getSimpleName(), doFn));
    }
  }
  /**
   * Reject state and timers {@link DoFn}.
   *
   * @param doFn the {@link DoFn} to possibly reject.
   */
  public static void rejectStateAndTimers(DoFn<?, ?> doFn) {
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    if (signature.stateDeclarations().size() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
              DoFn.StateId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              SparkRunner.class.getSimpleName()));
    }

    if (signature.timerDeclarations().size() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with timers in the %s.",
              DoFn.TimerId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              SparkRunner.class.getSimpleName()));
    }
  }

  public static <T> VoidFunction<T> emptyVoidFunction() {
    return new VoidFunction<T>() {
      @Override
      public void call(T t) throws Exception {
        // Empty implementation.
      }
    };
  }

  /**
   * A utility method that adapts {@link PairFunction} to a {@link PairFlatMapFunction} with an
   * {@link Iterator} input. This is particularly useful because it allows to use functions written
   * for mapToPair functions in flatmapToPair functions.
   *
   * @param pairFunction the {@link PairFunction} to adapt.
   * @param <T> the input type.
   * @param <K> the output key type.
   * @param <V> the output value type.
   * @return a {@link PairFlatMapFunction} that accepts an {@link Iterator} as an input and applies
   *     the {@link PairFunction} on every element.
   */
  public static <T, K, V> PairFlatMapFunction<Iterator<T>, K, V> pairFunctionToPairFlatMapFunction(
      final PairFunction<T, K, V> pairFunction) {
    return new PairFlatMapFunction<Iterator<T>, K, V>() {

      @Override
      public Iterable<Tuple2<K, V>> call(Iterator<T> itr) throws Exception {
        final Iterator<Tuple2<K, V>> outputItr =
            Iterators.transform(
                itr,
                new com.google.common.base.Function<T, Tuple2<K, V>>() {

                  @Override
                  public Tuple2<K, V> apply(T t) {
                    try {
                      return pairFunction.call(t);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
        return new Iterable<Tuple2<K, V>>() {

          @Override
          public Iterator<Tuple2<K, V>> iterator() {
            return outputItr;
          }
        };
      }
    };
  }

  /**
   * A utility method that adapts {@link Function} to a {@link FlatMapFunction} with an {@link
   * Iterator} input. This is particularly useful because it allows to use functions written for map
   * functions in flatmap functions.
   *
   * @param func the {@link Function} to adapt.
   * @param <InputT> the input type.
   * @param <OutputT> the output type.
   * @return a {@link FlatMapFunction} that accepts an {@link Iterator} as an input and applies the
   *     {@link Function} on every element.
   */
  public static <InputT, OutputT>
      FlatMapFunction<Iterator<InputT>, OutputT> functionToFlatMapFunction(
          final Function<InputT, OutputT> func) {
    return new FlatMapFunction<Iterator<InputT>, OutputT>() {

      @Override
      public Iterable<OutputT> call(Iterator<InputT> itr) throws Exception {
        final Iterator<OutputT> outputItr =
            Iterators.transform(
                itr,
                new com.google.common.base.Function<InputT, OutputT>() {

                  @Override
                  public OutputT apply(InputT t) {
                    try {
                      return func.call(t);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
        return new Iterable<OutputT>() {

          @Override
          public Iterator<OutputT> iterator() {
            return outputItr;
          }
        };
      }
    };
  }
}
