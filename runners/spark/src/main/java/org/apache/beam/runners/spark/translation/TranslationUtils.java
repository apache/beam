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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
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
   * A SparkCombineFn function applied to grouped KVs.
   *
   * @param <K> Grouped key type.
   * @param <InputT> Grouped values type.
   * @param <OutputT> Output type.
   */
  public static class CombineGroupedValues<K, InputT, OutputT>
      implements Function<WindowedValue<KV<K, Iterable<InputT>>>, WindowedValue<KV<K, OutputT>>> {
    private final SparkCombineFn<KV<K, InputT>, InputT, ?, OutputT> fn;

    public CombineGroupedValues(SparkCombineFn<KV<K, InputT>, InputT, ?, OutputT> fn) {
      this.fn = fn;
    }

    @SuppressWarnings("unchecked")
    @Override
    public WindowedValue<KV<K, OutputT>> call(WindowedValue<KV<K, Iterable<InputT>>> windowedKv)
        throws Exception {
      return WindowedValue.of(
          KV.of(
              windowedKv.getValue().getKey(),
              fn.getCombineFn()
                  .apply(windowedKv.getValue().getValue(), fn.ctxtForValue(windowedKv))),
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
    return pairDStream.map(Tuple2::_2);
  }

  /** {@link KV} to pair function. */
  public static <K, V> PairFunction<KV<K, V>, K, V> toPairFunction() {
    return kv -> new Tuple2<>(kv.getKey(), kv.getValue());
  }

  /** {@link KV} to pair flatmap function. */
  public static <K, V> PairFlatMapFunction<Iterator<KV<K, V>>, K, V> toPairFlatMapFunction() {
    return itr -> Iterators.transform(itr, kv -> new Tuple2<>(kv.getKey(), kv.getValue()));
  }

  /** A pair to {@link KV} function . */
  static class FromPairFunction<K, V>
      implements Function<Tuple2<K, V>, KV<K, V>>,
          org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function<
              Tuple2<K, V>, KV<K, V>> {
    @Override
    public KV<K, V> call(Tuple2<K, V> t2) {
      return KV.of(t2._1(), t2._2());
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public KV<K, V> apply(@Nonnull Tuple2<K, V> t2) {
      return call(t2);
    }
  }

  /** A pair to {@link KV} flatmap function . */
  static <K, V> FlatMapFunction<Iterator<Tuple2<K, V>>, KV<K, V>> fromPairFlatMapFunction() {
    return itr -> Iterators.transform(itr, t2 -> KV.of(t2._1(), t2._2()));
  }

  /** Extract key from a {@link WindowedValue} {@link KV} into a pair. */
  public static <K, V>
      PairFunction<WindowedValue<KV<K, V>>, ByteArray, WindowedValue<KV<K, V>>>
          toPairByKeyInWindowedValue(final Coder<K> keyCoder) {
    return windowedKv ->
        new Tuple2<>(
            new ByteArray(CoderHelpers.toByteArray(windowedKv.getValue().getKey(), keyCoder)),
            windowedKv);
  }

  /** Extract window from a {@link KV} with {@link WindowedValue} value. */
  static class ToKVByWindowInValueFunction<K, V>
      implements Function<KV<K, WindowedValue<V>>, WindowedValue<KV<K, V>>>,
          org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function<
              KV<K, WindowedValue<V>>, WindowedValue<KV<K, V>>> {

    @Override
    public WindowedValue<KV<K, V>> call(KV<K, WindowedValue<V>> kv) {
      WindowedValue<V> wv = kv.getValue();
      return wv.withValue(KV.of(kv.getKey(), wv.getValue()));
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public WindowedValue<KV<K, V>> apply(@Nonnull KV<K, WindowedValue<V>> kv) {
      return call(kv);
    }
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
      Iterable<PCollectionView<?>> views, EvaluationContext context) {
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
      Iterable<PCollectionView<?>> views, JavaSparkContext context, SparkPCollectionView pviews) {
    if (views == null) {
      return ImmutableMap.of();
    } else {
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs =
          Maps.newHashMap();
      for (PCollectionView<?> view : views) {
        SideInputBroadcast helper = pviews.getPCollectionView(view, context);
        WindowingStrategy<?, ?> windowingStrategy = view.getWindowingStrategyInternal();
        sideInputs.put(view.getTagInternal(), KV.of(windowingStrategy, helper));
      }
      return sideInputs;
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

    if (signature.timerDeclarations().size() > 0
        || signature.timerFamilyDeclarations().size() > 0) {
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
    return t -> {
      // Empty implementation.
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
    return itr ->
        Iterators.transform(
            itr,
            t -> {
              try {
                return pairFunction.call(t);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
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
    return itr ->
        Iterators.transform(
            itr,
            t -> {
              try {
                return func.call(t);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  /**
   * Utility to get mapping between TupleTag and a coder.
   *
   * @param outputs - A map of tuple tags and pcollections
   * @return mapping between TupleTag and a coder
   */
  public static Map<TupleTag<?>, Coder<WindowedValue<?>>> getTupleTagCoders(
      Map<TupleTag<?>, PValue> outputs) {
    Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMap = new HashMap<>(outputs.size());

    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      // we get the first PValue as all of them are fro the same type.
      PCollection<?> pCollection = (PCollection<?>) output.getValue();
      Coder<?> coder = pCollection.getCoder();
      Coder<? extends BoundedWindow> wCoder =
          pCollection.getWindowingStrategy().getWindowFn().windowCoder();
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<?>> windowedValueCoder =
          (Coder<WindowedValue<?>>) (Coder<?>) WindowedValue.getFullCoder(coder, wCoder);
      coderMap.put(output.getKey(), windowedValueCoder);
    }
    return coderMap;
  }

  /**
   * Returns a pair function to convert value to bytes via coder.
   *
   * @param coderMap - mapping between TupleTag and a coder
   * @return a pair function to convert value to bytes via coder
   */
  public static PairFunction<
          Tuple2<TupleTag<?>, WindowedValue<?>>,
          TupleTag<?>,
          ValueAndCoderLazySerializable<WindowedValue<?>>>
      getTupleTagEncodeFunction(final Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMap) {
    return tuple2 -> {
      TupleTag<?> tupleTag = tuple2._1;
      WindowedValue<?> windowedValue = tuple2._2;
      return new Tuple2<>(
          tupleTag, ValueAndCoderLazySerializable.of(windowedValue, coderMap.get(tupleTag)));
    };
  }

  /**
   * Returns a pair function to convert bytes to value via coder.
   *
   * @param coderMap - mapping between TupleTag and a coder
   * @return a pair function to convert bytes to value via coder
   */
  public static PairFunction<
          Tuple2<TupleTag<?>, ValueAndCoderLazySerializable<WindowedValue<?>>>,
          TupleTag<?>,
          WindowedValue<?>>
      getTupleTagDecodeFunction(final Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMap) {
    return tuple2 -> {
      TupleTag<?> tupleTag = tuple2._1;
      ValueAndCoderLazySerializable<WindowedValue<?>> windowedByteValue = tuple2._2;
      return new Tuple2<>(tupleTag, windowedByteValue.getOrDecode(coderMap.get(tupleTag)));
    };
  }

  /**
   * Check if we can avoid Serialization. It is only relevant to RDDs. DStreams are memory ser in
   * spark.
   *
   * <p>See <a
   * href="https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence">Spark
   * RDD programming guide</a> for more details.
   *
   * @param level StorageLevel required
   * @return true if the level is memory only
   */
  static boolean canAvoidRddSerialization(StorageLevel level) {
    return level.equals(StorageLevel.MEMORY_ONLY());
  }
}
