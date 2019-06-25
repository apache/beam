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
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import org.joda.time.Instant;

/**
 * A {@link org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn} with a {@link
 * org.apache.beam.sdk.transforms.CombineWithContext.Context} for the SparkRunner.
 */
public class SparkCombineFn<InputT, ValueT, AccumT, OutputT> extends SparkAbstractCombineFn {

  /** Accumulator of WindowedValues holding values for different windows. */
  static interface WindowedAccumulator<
      InputT, ValueT, AccumT, Impl extends WindowedAccumulator<InputT, ValueT, AccumT, Impl>> {

    enum Type {
      NON_MERGING,
      MERGING,
      SINGLE_WINDOW;

      boolean isMapBased() {
        return this == NON_MERGING || this == MERGING;
      }
    }

    /**
     * Check if this accumulator is empty.
     *
     * @return {@code true} if this accumulator is empty
     */
    boolean isEmpty();

    /** Add value with unexploded windows into the accumulator. */
    void add(WindowedValue<InputT> value, SparkCombineFn<InputT, ValueT, AccumT, ?> context)
        throws Exception;

    /**
     * Merge other acccumulator into this one.
     *
     * @param other the other accumulator to merge
     */
    void merge(Impl other, SparkCombineFn<?, ?, AccumT, ?> context) throws Exception;

    /** Extract output. */
    Collection<WindowedValue<AccumT>> extractOutput();

    /** Create concrete accumulator for given type. */
    static <InputT, ValueT, AccumT> WindowedAccumulator<InputT, ValueT, AccumT, ?> create(
        Function<InputT, ValueT> toValue, WindowingStrategy<?, ?> windowingStrategy) {
      Type type = getType(windowingStrategy);
      @SuppressWarnings("unchecked")
      TypeDescriptor<BoundedWindow> windowTypeDescriptor =
          (TypeDescriptor<BoundedWindow>) windowingStrategy.getWindowFn().getWindowTypeDescriptor();
      switch (type) {
        case MERGING:
          return MergingWindowedAccumulator.create(toValue, windowTypeDescriptor);
        case NON_MERGING:
          return NonMergingWindowedAccumulator.create(toValue);
        case SINGLE_WINDOW:
          return SingleWindowWindowedAccumulator.create(toValue);
        default:
          throw new IllegalArgumentException("Unknown type: " + type);
      }
    }

    /** Create concrete accumulator for given type. */
    static <InputT, ValueT, AccumT> WindowedAccumulator<InputT, ValueT, AccumT, ?> create(
        Function<InputT, ValueT> toValue,
        Type type,
        Iterable<WindowedValue<AccumT>> values,
        @Nullable TypeDescriptor<BoundedWindow> windowType) {
      switch (type) {
        case MERGING:
          return MergingWindowedAccumulator.from(toValue, values, windowType);
        case NON_MERGING:
          return NonMergingWindowedAccumulator.from(toValue, values);
        case SINGLE_WINDOW:
          return SingleWindowWindowedAccumulator.create(toValue);
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
  static class SingleWindowWindowedAccumulator<InputT, ValueT, AccumT>
      implements WindowedAccumulator<
          InputT, ValueT, AccumT, SingleWindowWindowedAccumulator<InputT, ValueT, AccumT>> {

    static <InputT, ValueT, AccumT> SingleWindowWindowedAccumulator<InputT, ValueT, AccumT> create(
        Function<InputT, ValueT> toValue) {
      return new SingleWindowWindowedAccumulator<>(toValue);
    }

    final Function<InputT, ValueT> toValue;
    WindowedValue<AccumT> windowAccumulator = null;

    SingleWindowWindowedAccumulator(Function<InputT, ValueT> toValue) {
      this.toValue = toValue;
    }

    @Override
    public void add(WindowedValue<InputT> value, SparkCombineFn<InputT, ValueT, AccumT, ?> context)
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
      AccumT result = context.combineFn.addInput(acc, toValue(value), ctx);
      Instant timestampCombined = combiner.combine(windowTimestamp, timestamp);
      windowAccumulator = WindowedValue.of(result, timestampCombined, window, PaneInfo.NO_FIRING);
    }

    @Override
    public void merge(
        SingleWindowWindowedAccumulator<InputT, ValueT, AccumT> other,
        SparkCombineFn<?, ?, AccumT, ?> context) {}

    @Override
    public Collection<WindowedValue<AccumT>> extractOutput() {
      return Arrays.asList(windowAccumulator);
    }

    ValueT toValue(WindowedValue<InputT> input) {
      try {
        return toValue.call(input.getValue());
      } catch (Exception ex) {
        throw UserCodeException.wrap(ex);
      }
    }

    @Override
    public boolean isEmpty() {
      return windowAccumulator == null;
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
          InputT,
          ValueT,
          AccumT,
          Impl extends MapBasedWindowedAccumulator<InputT, ValueT, AccumT, Impl>>
      implements WindowedAccumulator<InputT, ValueT, AccumT, Impl> {

    final Function<InputT, ValueT> toValue;
    final Map<BoundedWindow, WindowedValue<AccumT>> map;

    MapBasedWindowedAccumulator(
        Function<InputT, ValueT> toValue, Map<BoundedWindow, WindowedValue<AccumT>> map) {
      this.toValue = toValue;
      this.map = map;
    }

    @Override
    public void add(WindowedValue<InputT> value, SparkCombineFn<InputT, ValueT, AccumT, ?> context)
        throws Exception {
      for (WindowedValue<InputT> v : value.explodeWindows()) {
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
              AccumT result = context.combineFn.addInput(acc, toValue(v), ctx);
              Instant timestampCombined = combiner.combine(windowTimestamp, timestamp);
              return WindowedValue.of(result, timestampCombined, window, PaneInfo.NO_FIRING);
            });
      }
      mergeWindows(context);
    }

    @Override
    public void merge(Impl other, SparkCombineFn<?, ?, AccumT, ?> context) throws Exception {
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

    @Override
    public boolean isEmpty() {
      return map.isEmpty();
    }

    void mergeWindows(SparkCombineFn<?, ?, AccumT, ?> fn) throws Exception {}

    private ValueT toValue(WindowedValue<InputT> value) {
      try {
        return toValue.call(value.getValue());
      } catch (Exception ex) {
        throw UserCodeException.wrap(ex);
      }
    }
  }

  /** Accumulator for non-merging windows. */
  static class NonMergingWindowedAccumulator<InputT, ValueT, AccumT>
      extends MapBasedWindowedAccumulator<
          InputT, ValueT, AccumT, NonMergingWindowedAccumulator<InputT, ValueT, AccumT>> {

    static <InputT, ValueT, AccumT> NonMergingWindowedAccumulator<InputT, ValueT, AccumT> create(
        Function<InputT, ValueT> toValue) {
      return new NonMergingWindowedAccumulator<>(toValue);
    }

    static <InputT, ValueT, AccumT> NonMergingWindowedAccumulator<InputT, ValueT, AccumT> from(
        Function<InputT, ValueT> toValue, Iterable<WindowedValue<AccumT>> values) {
      return new NonMergingWindowedAccumulator<>(toValue, values);
    }

    @SuppressWarnings("unchecked")
    private NonMergingWindowedAccumulator(Function<InputT, ValueT> toValue) {
      super(toValue, new HashMap<>());
    }

    private NonMergingWindowedAccumulator(
        Function<InputT, ValueT> toValue, Iterable<WindowedValue<AccumT>> values) {
      super(toValue, asMap(values, new HashMap<>()));
    }
  }

  /** Accumulator for merging windows. */
  static class MergingWindowedAccumulator<InputT, ValueT, AccumT>
      extends MapBasedWindowedAccumulator<
          InputT, ValueT, AccumT, MergingWindowedAccumulator<InputT, ValueT, AccumT>> {

    static <InputT, ValueT, AccumT> MergingWindowedAccumulator<InputT, ValueT, AccumT> create(
        Function<InputT, ValueT> toValue, @Nullable TypeDescriptor<BoundedWindow> windowType) {
      return new MergingWindowedAccumulator<>(toValue, windowType);
    }

    static <InputT, ValueT, AccumT> MergingWindowedAccumulator<InputT, ValueT, AccumT> from(
        Function<InputT, ValueT> toValue,
        Iterable<WindowedValue<AccumT>> values,
        @Nullable TypeDescriptor<BoundedWindow> windowType) {
      return new MergingWindowedAccumulator<>(toValue, values, windowType);
    }

    @SuppressWarnings("unchecked")
    private MergingWindowedAccumulator(
        Function<InputT, ValueT> toValue, @Nullable TypeDescriptor<?> windowType) {
      super(toValue, new TreeMap<>(asWindowComparator(windowType)));
    }

    private MergingWindowedAccumulator(
        Function<InputT, ValueT> toValue,
        Iterable<WindowedValue<AccumT>> values,
        @Nullable TypeDescriptor<BoundedWindow> windowType) {
      super(toValue, asMap(values, new TreeMap<>(asWindowComparator(windowType))));
    }

    @Override
    void mergeWindows(SparkCombineFn<?, ?, AccumT, ?> fn) throws Exception {

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

  static class WindowedAccumulatorCoder<InputT, ValueT, AccumT>
      extends Coder<WindowedAccumulator<InputT, ValueT, AccumT, ?>> {

    static <InputT, ValueT, AccumT> WindowedAccumulatorCoder<InputT, ValueT, AccumT> of(
        Function<InputT, ValueT> toValue,
        Coder<BoundedWindow> windowCoder,
        Coder<AccumT> accumCoder,
        WindowingStrategy windowingStrategy) {
      return new WindowedAccumulatorCoder<>(
          toValue, windowCoder, accumCoder, getType(windowingStrategy));
    }

    private final Function<InputT, ValueT> toValue;
    private final @Nullable TypeDescriptor<BoundedWindow> windowDesc;
    private final IterableCoder<WindowedValue<AccumT>> wrap;
    private final Coder<WindowedValue<AccumT>> accumCoder;
    private final WindowedAccumulator.Type type;

    @SuppressWarnings("unchecked")
    WindowedAccumulatorCoder(
        Function<InputT, ValueT> toValue,
        Coder<BoundedWindow> windowCoder,
        Coder<AccumT> accumCoder,
        WindowedAccumulator.Type type) {
      this.toValue = toValue;
      this.accumCoder = WindowedValue.FullWindowedValueCoder.of(accumCoder, windowCoder);
      this.wrap = IterableCoder.of(this.accumCoder);
      windowDesc = windowCoder.getEncodedTypeDescriptor();
      this.type = type;
    }

    @Override
    public void encode(WindowedAccumulator<InputT, ValueT, AccumT, ?> value, OutputStream outStream)
        throws CoderException, IOException {
      if (type.isMapBased()) {
        wrap.encode(((MapBasedWindowedAccumulator<?, ?, AccumT, ?>) value).map.values(), outStream);
      } else {
        accumCoder.encode(
            ((SingleWindowWindowedAccumulator<?, ?, AccumT>) value).windowAccumulator, outStream);
      }
    }

    @Override
    public WindowedAccumulator<InputT, ValueT, AccumT, ?> decode(InputStream inStream)
        throws CoderException, IOException {
      if (type.isMapBased()) {
        return WindowedAccumulator.create(toValue, type, wrap.decode(inStream), windowDesc);
      }
      return WindowedAccumulator.create(
          toValue, type, Arrays.asList(accumCoder.decode(inStream)), windowDesc);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return wrap.getComponents();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  public static <K, V, AccumT, OutputT> SparkCombineFn<KV<K, V>, V, AccumT, OutputT> keyed(
      CombineWithContext.CombineFnWithContext<V, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SparkCombineFn<>(KV::getValue, combineFn, options, sideInputs, windowingStrategy);
  }

  public static <InputT, AccumT, OutputT> SparkCombineFn<InputT, InputT, AccumT, OutputT> globally(
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SparkCombineFn<>(e -> e, combineFn, options, sideInputs, windowingStrategy);
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

  private final Function<InputT, ValueT> toValue;
  private final CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> combineFn;

  public SparkCombineFn(
      Function<InputT, ValueT> toValue,
      CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, sideInputs, windowingStrategy);
    this.toValue = toValue;
    this.combineFn = combineFn;
  }

  /** Create empty combiner. Implements Spark's zeroValue for aggregateFn. */
  WindowedAccumulator<InputT, ValueT, AccumT, ?> createCombiner() {
    return WindowedAccumulator.create(toValue, windowingStrategy);
  }

  /**
   * Implements Spark's createCombiner function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  WindowedAccumulator<InputT, ValueT, AccumT, ?> createCombiner(WindowedValue<InputT> value) {
    try {
      WindowedAccumulator<InputT, ValueT, AccumT, ?> accumulator =
          WindowedAccumulator.create(toValue, windowingStrategy);
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
  WindowedAccumulator<InputT, ValueT, AccumT, ?> mergeValue(
      WindowedAccumulator<InputT, ValueT, AccumT, ?> accumulator, WindowedValue<InputT> value) {
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
  WindowedAccumulator<InputT, ValueT, AccumT, ?> mergeCombiners(
      WindowedAccumulator ac1, WindowedAccumulator ac2) {
    try {
      ac1.merge(ac2, this);
      return ac1;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  Iterable<WindowedValue<OutputT>> extractOutput(WindowedAccumulator<?, ?, AccumT, ?> accumulator) {
    return accumulator.extractOutput().stream()
        .map(
            windowAcc ->
                windowAcc.withValue(
                    combineFn.extractOutput(windowAcc.getValue(), ctxtForInput(windowAcc))))
        .collect(Collectors.toList());
  }

  WindowedAccumulatorCoder<InputT, ValueT, AccumT> accumulatorCoder(
      Coder<BoundedWindow> windowCoder,
      Coder<AccumT> accumulatorCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    return WindowedAccumulatorCoder.of(toValue, windowCoder, accumulatorCoder, windowingStrategy);
  }

  CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> getCombineFn() {
    return combineFn;
  }
}
