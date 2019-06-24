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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

/**
 * A {@link org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn} with a {@link
 * org.apache.beam.sdk.transforms.CombineWithContext.Context} for the SparkRunner.
 */
public class SparkKeyedCombineFn<K, InputT, AccumT, OutputT> extends SparkAbstractCombineFn {

  static class WindowedAccumulatorCoder<AccumT> extends Coder<WindowedAccumulator<AccumT>> {

    static <T> WindowedAccumulatorCoder<T> of(
        Coder<BoundedWindow> windowCoder, Coder<T> accumCoder) {
      return new WindowedAccumulatorCoder<>(windowCoder, accumCoder);
    }

    private final @Nullable TypeDescriptor<?> windowDesc;
    private final IterableCoder<WindowedValue<AccumT>> wrap;

    WindowedAccumulatorCoder(Coder<? extends BoundedWindow> windowCoder, Coder<AccumT> accumCoder) {
      this.wrap =
          IterableCoder.of(WindowedValue.FullWindowedValueCoder.of(accumCoder, windowCoder));
      windowDesc = windowCoder.getEncodedTypeDescriptor();
    }

    @Override
    public void encode(WindowedAccumulator value, OutputStream outStream)
        throws CoderException, IOException {
      wrap.encode(value.map.values(), outStream);
    }

    @Override
    public WindowedAccumulator decode(InputStream inStream) throws CoderException, IOException {
      return WindowedAccumulator.from(
          StreamSupport.stream(wrap.decode(inStream).spliterator(), false)
              .collect(
                  Collectors.toMap(
                      w -> Iterables.getOnlyElement(w.getWindows()),
                      Function.identity(),
                      (a, b) -> a,
                      () -> new TreeMap<>(asWindowComparator(windowDesc)))));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return wrap.getComponents();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  /** Accumulator of WindowedValues holding values for different windows. */
  static class WindowedAccumulator<AccumT> {

    static <T> WindowedAccumulator<T> create(@Nullable TypeDescriptor<BoundedWindow> windowType) {
      return new WindowedAccumulator<>(windowType);
    }

    static <T> WindowedAccumulator<T> from(SortedMap<BoundedWindow, WindowedValue<T>> map) {
      return new WindowedAccumulator<>(map);
    }

    final SortedMap<BoundedWindow, WindowedValue<AccumT>> map;

    @SuppressWarnings("unchecked")
    private WindowedAccumulator(@Nullable TypeDescriptor<?> windowType) {
      Comparator<BoundedWindow> comparator = asWindowComparator(windowType);
      this.map = new TreeMap<>(comparator);
    }

    private WindowedAccumulator(SortedMap<BoundedWindow, WindowedValue<AccumT>> map) {
      this.map = map;
    }

    /** Add value with unexploded windows into the accumulator. */
    <K, InputT> void add(
        WindowedValue<KV<K, InputT>> value, SparkKeyedCombineFn<K, InputT, AccumT, ?> context)
        throws Exception {
      for (WindowedValue<KV<K, InputT>> v : value.explodeWindows()) {
        SparkCombineContext ctx = context.ctxtForInput(v);
        BoundedWindow window = Iterables.getOnlyElement(v.getWindows());
        TimestampCombiner combiner = context.windowingStrategy.getTimestampCombiner();
        Instant windowTimestamp =
            combiner.assign(
                window,
                context.windowingStrategy.getWindowFn().getOutputTime(v.getTimestamp(), window));
        map.compute(
            window,
            (w, windowAccumulator) -> {
              final AccumT acc;
              final Instant timestamp;
              if (windowAccumulator == null) {
                acc = context.combineFn.createAccumulator(ctx);
                timestamp = windowTimestamp;
              } else {
                acc = windowAccumulator.getValue();
                timestamp = windowAccumulator.getTimestamp();
              }
              AccumT result = context.combineFn.addInput(acc, v.getValue().getValue(), ctx);
              Instant timestampCombined = combiner.combine(windowTimestamp, timestamp);
              return WindowedValue.of(result, timestampCombined, window, PaneInfo.NO_FIRING);
            });
      }
      mergeWindows(context);
    }

    /**
     * Merge other acccumulator into this one.
     *
     * @param other the other accumulator to merge
     */
    void merge(WindowedAccumulator<AccumT> other, SparkKeyedCombineFn<?, ?, AccumT, ?> context)
        throws Exception {
      other.map.forEach(
          (window, acc) -> {
            WindowedValue<AccumT> thisAcc = this.map.get(window);
            if (thisAcc == null) {
              // just copy
              this.map.put(window, acc);
            } else {
              // merge
              this.map.put(
                  window,
                  WindowedValue.of(
                      context.combineFn.mergeAccumulators(
                          Lists.newArrayList(thisAcc.getValue(), acc.getValue()),
                          context.ctxtForInput(acc)),
                      context
                          .windowingStrategy
                          .getTimestampCombiner()
                          .combine(acc.getTimestamp(), thisAcc.getTimestamp()),
                      window,
                      PaneInfo.NO_FIRING));
            }
          });
      mergeWindows(context);
    }

    private void mergeWindows(SparkKeyedCombineFn<?, ?, AccumT, ?> fn) throws Exception {

      if (fn.windowingStrategy.getWindowFn().isNonMerging()) {
        return;
      }

      SparkCombineContext ctx = fn.ctxForWindows(this.map.keySet());

      @SuppressWarnings("unchecked")
      WindowFn<Object, BoundedWindow> windowFn = (WindowFn) fn.windowingStrategy.getWindowFn();
      windowFn.mergeWindows(
          asMergeContext(
              windowFn,
              (a, b) -> fn.combineFn.mergeAccumulators(Lists.newArrayList(a, b), ctx),
              (toBeMerged, mergeResult) -> {
                Instant mergedInstant =
                    fn.windowingStrategy
                        .getTimestampCombiner()
                        .merge(
                            mergeResult.getKey(),
                            toBeMerged.stream()
                                .map(w -> map.get(w).getTimestamp())
                                .collect(Collectors.toList()));
                toBeMerged.forEach(this.map::remove);
                this.map.put(
                    mergeResult.getKey(),
                    WindowedValue.of(
                        mergeResult.getValue(),
                        mergedInstant,
                        mergeResult.getKey(),
                        PaneInfo.NO_FIRING));
              },
              map));
    }

    private WindowFn<Object, BoundedWindow>.MergeContext asMergeContext(
        WindowFn<Object, BoundedWindow> windowFn,
        BiFunction<AccumT, AccumT, AccumT> mergeFn,
        BiConsumer<Collection<BoundedWindow>, KV<BoundedWindow, AccumT>> afterMerge,
        SortedMap<BoundedWindow, WindowedValue<AccumT>> map) {

      return windowFn.new MergeContext() {

        @Override
        public Collection<BoundedWindow> windows() {
          return map.keySet();
        }

        @Override
        public void merge(Collection<BoundedWindow> toBeMerged, BoundedWindow mergeResult) {
          AccumT accumulator = null;
          for (BoundedWindow w : toBeMerged) {
            WindowedValue<AccumT> windowAccumulator = Objects.requireNonNull(map.get(w));
            if (accumulator == null) {
              accumulator = windowAccumulator.getValue();
            } else {
              accumulator = mergeFn.apply(accumulator, windowAccumulator.getValue());
            }
          }
          afterMerge.accept(toBeMerged, KV.of(mergeResult, accumulator));
        }
      };
    }

    @Override
    public String toString() {
      return "WindowedAccumulator(" + this.map + ")";
    }
  }

  private static Comparator<BoundedWindow> asWindowComparator(TypeDescriptor<?> windowType) {
    final Comparator<BoundedWindow> comparator;
    if (windowType != null
        && StreamSupport.stream(windowType.getInterfaces().spliterator(), false)
            .anyMatch(t -> t.isSubtypeOf(TypeDescriptor.of(Comparable.class)))) {
      comparator = (Comparator) Comparator.naturalOrder();
    } else {
      comparator = Comparator.comparing(BoundedWindow::maxTimestamp);
    }
    return comparator;
  }

  private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;

  public SparkKeyedCombineFn(
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, sideInputs, windowingStrategy);
    this.combineFn = combineFn;
  }

  /** Applying the combine function directly on a key's grouped values - post grouping. */
  public OutputT apply(WindowedValue<KV<K, Iterable<InputT>>> windowedKv) {
    // apply combine function on grouped values.
    return combineFn.apply(windowedKv.getValue().getValue(), ctxtForInput(windowedKv));
  }

  /**
   * Implements Spark's createCombiner function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  WindowedAccumulator<AccumT> createCombiner(WindowedValue<KV<K, InputT>> value) {
    try {
      WindowedAccumulator accumulator =
          new WindowedAccumulator(windowingStrategy.getWindowFn().getWindowTypeDescriptor());
      accumulator.add(value, this);
      return accumulator;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Implements Spark's mergeValue function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  WindowedAccumulator mergeValue(
      WindowedAccumulator accumulator, WindowedValue<KV<K, InputT>> value) {
    try {
      accumulator.add(value, this);
      return accumulator;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Implements Spark's mergeCombiners function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  WindowedAccumulator<AccumT> mergeCombiners(
      WindowedAccumulator<AccumT> ac1, WindowedAccumulator<AccumT> ac2) {
    try {
      ac1.merge(ac2, this);
      return ac1;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  Iterable<WindowedValue<OutputT>> extractOutput(WindowedAccumulator<AccumT> accumulator) {
    return accumulator.map.entrySet().stream()
        .map(
            entry -> {
              AccumT windowAcc = entry.getValue().getValue();
              return entry
                  .getValue()
                  .withValue(combineFn.extractOutput(windowAcc, ctxtForInput(entry.getValue())));
            })
        .collect(Collectors.toList());
  }
}
