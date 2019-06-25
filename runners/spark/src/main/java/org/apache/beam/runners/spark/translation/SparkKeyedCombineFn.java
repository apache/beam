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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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

  /** Accumulator of WindowedValues holding values for different windows. */
  static interface WindowedAccumulator<AccumT, Impl extends WindowedAccumulator<AccumT, Impl>> {

    enum Type {
      NON_MERGING,
      MERGING,
      SINGLE_WINDOW;

      boolean isMapBased() {
        return this == NON_MERGING || this == MERGING;
      }
    }

    /** Add value with unexploded windows into the accumulator. */
    <K, InputT> void add(
        WindowedValue<KV<K, InputT>> value, SparkKeyedCombineFn<K, InputT, AccumT, ?> context)
        throws Exception;

    /**
     * Merge other acccumulator into this one.
     *
     * @param other the other accumulator to merge
     */
    void merge(Impl other, SparkKeyedCombineFn<?, ?, AccumT, ?> context) throws Exception;

    /** Extract output. */
    Collection<WindowedValue<AccumT>> extractOutput();

    /** Create concrete accumulator for given type. */
    static <AccumT> WindowedAccumulator<AccumT, ?> create(
        WindowingStrategy<?, ?> windowingStrategy) {
      Type type = getType(windowingStrategy);
      @SuppressWarnings("unchecked")
      TypeDescriptor<BoundedWindow> windowTypeDescriptor =
          (TypeDescriptor<BoundedWindow>) windowingStrategy.getWindowFn().getWindowTypeDescriptor();
      switch (type) {
        case MERGING:
          return MergingWindowedAccumulator.create(windowTypeDescriptor);
        case NON_MERGING:
          return NonMergingWindowedAccumulator.create();
        case SINGLE_WINDOW:
          return SingleWindowWindowedAccumulator.create();
        default:
          throw new IllegalArgumentException("Unknown type: " + type);
      }
    }

    /** Create concrete accumulator for given type. */
    static <AccumT> WindowedAccumulator<AccumT, ?> create(
        Type type,
        Iterable<WindowedValue<AccumT>> values,
        @Nullable TypeDescriptor<BoundedWindow> windowType) {
      switch (type) {
        case MERGING:
          return MergingWindowedAccumulator.from(values, windowType);
        case NON_MERGING:
          return NonMergingWindowedAccumulator.from(values);
        case SINGLE_WINDOW:
          return SingleWindowWindowedAccumulator.create();
        default:
          throw new IllegalArgumentException("Unknown type: " + type);
      }
    }
  }

  /**
   * Accumulator for {@link WindowFn WindowFns} which create only single window per key, and
   * therefore don't need any map to hold different windows per key.
   *
   * @param <AccumT> type of accumulator
   */
  static class SingleWindowWindowedAccumulator<AccumT>
      implements WindowedAccumulator<AccumT, SingleWindowWindowedAccumulator<AccumT>> {

    static <AccumT> SingleWindowWindowedAccumulator<AccumT> create() {
      return new SingleWindowWindowedAccumulator<>();
    }

    WindowedValue<AccumT> windowAccumulator = null;

    @Override
    public <K, InputT> void add(
        WindowedValue<KV<K, InputT>> value, SparkKeyedCombineFn<K, InputT, AccumT, ?> context)
        throws Exception {
      BoundedWindow window = Iterables.getOnlyElement(value.getWindows());
      SparkCombineContext ctx = context.ctxtForInput(value);
      TimestampCombiner combiner = context.windowingStrategy.getTimestampCombiner();
      Instant windowTimestamp =
          combiner.assign(
              window,
              context.windowingStrategy.getWindowFn().getOutputTime(value.getTimestamp(), window));
      final AccumT acc;
      final Instant timestamp;
      if (windowAccumulator == null) {
        acc = context.combineFn.createAccumulator(ctx);
        timestamp = windowTimestamp;
      } else {
        acc = windowAccumulator.getValue();
        timestamp = windowAccumulator.getTimestamp();
      }
      AccumT result = context.combineFn.addInput(acc, value.getValue().getValue(), ctx);
      Instant timestampCombined = combiner.combine(windowTimestamp, timestamp);
      windowAccumulator = WindowedValue.of(result, timestampCombined, window, PaneInfo.NO_FIRING);
    }

    @Override
    public void merge(
        SingleWindowWindowedAccumulator<AccumT> other,
        SparkKeyedCombineFn<?, ?, AccumT, ?> context) {}

    @Override
    public Collection<WindowedValue<AccumT>> extractOutput() {
      return Arrays.asList(windowAccumulator);
    }
  }

  /**
   * Abstract base class for {@link WindowedAccumulator WindowedAccumulators} which hold
   * accumulators in map.
   *
   * @param <AccumT> type of accumulator
   * @param <Impl> type of final subclass
   */
  abstract static class MapBasedWindowedAccumulator<
          AccumT, Impl extends MapBasedWindowedAccumulator<AccumT, Impl>>
      implements WindowedAccumulator<AccumT, Impl> {

    final Map<BoundedWindow, WindowedValue<AccumT>> map;

    MapBasedWindowedAccumulator(Map<BoundedWindow, WindowedValue<AccumT>> map) {
      this.map = map;
    }

    @Override
    public <K, InputT> void add(
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

    @Override
    public void merge(Impl other, SparkKeyedCombineFn<?, ?, AccumT, ?> context) throws Exception {
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

    @Override
    public Collection<WindowedValue<AccumT>> extractOutput() {
      return map.values();
    }

    void mergeWindows(SparkKeyedCombineFn<?, ?, AccumT, ?> fn) throws Exception {}
  }

  /** Accumulator for non-merging windows. */
  static class NonMergingWindowedAccumulator<AccumT>
      extends MapBasedWindowedAccumulator<AccumT, NonMergingWindowedAccumulator<AccumT>> {

    static <T> NonMergingWindowedAccumulator<T> create() {
      return new NonMergingWindowedAccumulator<>();
    }

    static <T> NonMergingWindowedAccumulator<T> from(Iterable<WindowedValue<T>> values) {
      return new NonMergingWindowedAccumulator<>(values);
    }

    @SuppressWarnings("unchecked")
    private NonMergingWindowedAccumulator() {
      super(new HashMap<>());
    }

    private NonMergingWindowedAccumulator(Iterable<WindowedValue<AccumT>> values) {
      super(asMap(values, new HashMap<>()));
    }
  }

  /**
   * Accumulator for merging windows.
   *
   * @param <AccumT> type of accumulator
   */
  static class MergingWindowedAccumulator<AccumT>
      extends MapBasedWindowedAccumulator<AccumT, MergingWindowedAccumulator<AccumT>> {

    static <T> MergingWindowedAccumulator<T> create(
        @Nullable TypeDescriptor<BoundedWindow> windowType) {
      return new MergingWindowedAccumulator<>(windowType);
    }

    static <T> MergingWindowedAccumulator<T> from(
        Iterable<WindowedValue<T>> values, @Nullable TypeDescriptor<BoundedWindow> windowType) {
      return new MergingWindowedAccumulator<>(values, windowType);
    }

    @SuppressWarnings("unchecked")
    private MergingWindowedAccumulator(@Nullable TypeDescriptor<?> windowType) {
      super(new TreeMap<>(asWindowComparator(windowType)));
    }

    private MergingWindowedAccumulator(
        Iterable<WindowedValue<AccumT>> values,
        @Nullable TypeDescriptor<BoundedWindow> windowType) {
      super(asMap(values, new TreeMap<>(asWindowComparator(windowType))));
    }

    @Override
    void mergeWindows(SparkKeyedCombineFn<?, ?, AccumT, ?> fn) throws Exception {

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
        Map<BoundedWindow, WindowedValue<AccumT>> map) {

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
      return "MergingWindowedAccumulator(" + this.map + ")";
    }
  }

  static class WindowedAccumulatorCoder<AccumT> extends Coder<WindowedAccumulator<AccumT, ?>> {

    static <T> WindowedAccumulatorCoder<T> of(
        Coder<BoundedWindow> windowCoder, Coder<T> accumCoder, WindowingStrategy windowingStrategy) {
      return new WindowedAccumulatorCoder<>(windowCoder, accumCoder, getType(windowingStrategy));
    }

    private final @Nullable TypeDescriptor<BoundedWindow> windowDesc;
    private final IterableCoder<WindowedValue<AccumT>> wrap;
    private final Coder<WindowedValue<AccumT>> accumCoder;
    private final WindowedAccumulator.Type type;

    @SuppressWarnings("unchecked")
    WindowedAccumulatorCoder(
        Coder<? extends BoundedWindow> windowCoder,
        Coder<AccumT> accumCoder,
        WindowedAccumulator.Type type) {
      this.accumCoder = WindowedValue.FullWindowedValueCoder.of(accumCoder, windowCoder);
      this.wrap = IterableCoder.of(this.accumCoder);
      windowDesc = (TypeDescriptor) windowCoder.getEncodedTypeDescriptor();
      this.type = type;
    }

    @Override
    public void encode(WindowedAccumulator<AccumT, ?> value, OutputStream outStream)
        throws CoderException, IOException {
      if (type.isMapBased()) {
        wrap.encode(((MapBasedWindowedAccumulator<AccumT, ?>) value).map.values(), outStream);
      } else {
        accumCoder.encode(
            ((SingleWindowWindowedAccumulator<AccumT>) value).windowAccumulator, outStream);
      }
    }

    @Override
    public WindowedAccumulator decode(InputStream inStream) throws CoderException, IOException {
      if (type.isMapBased()) {
        return WindowedAccumulator.create(type, wrap.decode(inStream), windowDesc);
      }
      return WindowedAccumulator.create(
          type, Arrays.asList(accumCoder.decode(inStream)), windowDesc);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return wrap.getComponents();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  private static <T> Map<BoundedWindow, WindowedValue<T>> asMap(
      Iterable<WindowedValue<T>> values, Map<BoundedWindow, WindowedValue<T>> res) {
    for (WindowedValue<T> v : values) {
      res.put(Iterables.getOnlyElement(v.getWindows()), v);
    }
    return res;
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

  private static WindowedAccumulator.Type getType(WindowingStrategy<?, ?> windowingStrategy) {
    if (windowingStrategy.getWindowFn().isNonMerging()) {
      if (windowingStrategy.getWindowFn().assignsToOneWindow()) {
        // FIXME: need to incorporate window in the key for this to  work
        // return WindowedAccumulator.Type.SINGLE_WINDOW;
      }
      return WindowedAccumulator.Type.NON_MERGING;
    }
    return WindowedAccumulator.Type.MERGING;
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
  WindowedAccumulator<AccumT, ?> createCombiner(WindowedValue<KV<K, InputT>> value) {
    try {
      WindowedAccumulator<AccumT, ?> accumulator = WindowedAccumulator.create(windowingStrategy);
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  WindowedAccumulator<AccumT, ?> mergeCombiners(WindowedAccumulator ac1, WindowedAccumulator ac2) {
    try {
      ac1.merge(ac2, this);
      return ac1;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  Iterable<WindowedValue<OutputT>> extractOutput(WindowedAccumulator<AccumT, ?> accumulator) {
    return accumulator.extractOutput().stream()
        .map(
            windowAcc ->
                windowAcc.withValue(
                    combineFn.extractOutput(windowAcc.getValue(), ctxtForInput(windowAcc))))
        .collect(Collectors.toList());
  }
}
