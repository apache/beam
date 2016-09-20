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
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

/**
 * A set of utilities to help translating Beam transformations into Spark transformations.
 */
public final class TranslationUtils {

  private TranslationUtils() {
  }

  /**
   * In-memory state internals factory.
   *
   * @param <K> State key type.
   */
  static class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K>,
      Serializable {
    @Override
    public StateInternals<K> stateInternalsForKey(K key) {
      return InMemoryStateInternals.forKey(key);
    }
  }

  /**
   * A {@link Combine.GroupedValues} function applied to grouped KVs.
   *
   * @param <K>       Grouped key type.
   * @param <InputT>  Grouped values type.
   * @param <OutputT> Output type.
   */
  public static class CombineGroupedValues<K, InputT, OutputT> implements
      Function<WindowedValue<KV<K, Iterable<InputT>>>, WindowedValue<KV<K, OutputT>>> {
    private final Combine.KeyedCombineFn<K, InputT, ?, OutputT> keyed;

    public CombineGroupedValues(Combine.GroupedValues<K, InputT, OutputT> transform) {
      //noinspection unchecked
      keyed = (Combine.KeyedCombineFn<K, InputT, ?, OutputT>) transform.getFn();
    }

    @Override
    public WindowedValue<KV<K, OutputT>> call(WindowedValue<KV<K, Iterable<InputT>>> windowedKv)
        throws Exception {
      KV<K, Iterable<InputT>> kv = windowedKv.getValue();
      return WindowedValue.of(KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue())),
          windowedKv.getTimestamp(), windowedKv.getWindows(), windowedKv.getPane());
    }
  }

  /**
   * Checks if the window transformation should be applied or skipped.
   *
   * <p>
   * Avoid running assign windows if both source and destination are global window
   * or if the user has not specified the WindowFn (meaning they are just messing
   * with triggering or allowed lateness).
   * </p>
   *
   * @param transform The {@link Window.Bound} transformation.
   * @param context   The {@link EvaluationContext}.
   * @param <T>       PCollection type.
   * @param <W>       {@link BoundedWindow} type.
   * @return if to apply the transformation.
   */
  public static <T, W extends BoundedWindow> boolean
  skipAssignWindows(Window.Bound<T> transform, EvaluationContext context) {
    @SuppressWarnings("unchecked")
    WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();
    return windowFn == null
        || (context.getInput(transform).getWindowingStrategy().getWindowFn()
            instanceof GlobalWindows
                && windowFn instanceof GlobalWindows);
  }

  /** Transform a pair stream into a value stream. */
  public static <T1, T2> JavaDStream<T2> dStreamValues(JavaPairDStream<T1, T2> pairDStream) {
    return pairDStream.map(new Function<Tuple2<T1, T2>, T2>() {
      @Override
      public T2 call(Tuple2<T1, T2> v1) throws Exception {
        return v1._2();
      }
    });
  }

  /** {@link KV} to pair function. */
  static <K, V> PairFunction<KV<K, V>, K, V> toPairFunction() {
    return new PairFunction<KV<K, V>, K, V>() {
      @Override
      public Tuple2<K, V> call(KV<K, V> kv) {
        return new Tuple2<>(kv.getKey(), kv.getValue());
      }
    };
  }

  /**  A pair to {@link KV} function . */
  static <K, V> Function<Tuple2<K, V>, KV<K, V>> fromPairFunction() {
    return new Function<Tuple2<K, V>, KV<K, V>>() {
      @Override
      public KV<K, V> call(Tuple2<K, V> t2) {
        return KV.of(t2._1(), t2._2());
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

  /***
   * Create SideInputs as Broadcast variables.
   *
   * @param views   The {@link PCollectionView}s.
   * @param context The {@link EvaluationContext}.
   * @return a map of tagged {@link BroadcastHelper}s.
   */
  public static Map<TupleTag<?>, BroadcastHelper<?>> getSideInputs(List<PCollectionView<?>> views,
      EvaluationContext context) {
    if (views == null) {
      return ImmutableMap.of();
    } else {
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = Maps.newHashMap();
      for (PCollectionView<?> view : views) {
        Iterable<? extends WindowedValue<?>> collectionView = context.getPCollectionView(view);
        Coder<Iterable<WindowedValue<?>>> coderInternal = view.getCoderInternal();
        @SuppressWarnings("unchecked")
        BroadcastHelper<?> helper =
            BroadcastHelper.create((Iterable<WindowedValue<?>>) collectionView, coderInternal);
        //broadcast side inputs
        helper.broadcast(context.getSparkContext());
        sideInputs.put(view.getTagInternal(), helper);
      }
      return sideInputs;
    }
  }

}
