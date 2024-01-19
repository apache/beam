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
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SparkSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn} with a {@link
 * org.apache.beam.sdk.transforms.CombineWithContext.Context} for the SparkRunner.
 */
public class SparkCombineFn<InputT, ValueT, AccumT, OutputT> implements Serializable {

  /** Accumulator of WindowedValues holding values for different windows. */
  public interface WindowedAccumulator<
      InputT, ValueT, AccumT, ImplT extends WindowedAccumulator<InputT, ValueT, AccumT, ImplT>> {

    /**
     * Type of the accumulator. The type depends (mostly) on {@link WindowingStrategy}, and more
     * specialized versions enable more optimizations.
     */
    enum Type {
      MERGING,
      NON_MERGING,
      EXPLODE_WINDOWS,
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
     * Merge other accumulator into this one.
     *
     * @param other the other accumulator to merge
     */
    void merge(ImplT other, SparkCombineFn<?, ?, AccumT, ?> context) throws Exception;

    /** Extract output. */
    Collection<WindowedValue<AccumT>> extractOutput();

    /** Create concrete accumulator for given type. */
    static <InputT, ValueT, AccumT> WindowedAccumulator<InputT, ValueT, AccumT, ?> create(
        SparkCombineFn<InputT, ValueT, AccumT, ?> context,
        Function<InputT, ValueT> toValue,
        WindowingStrategy<?, ?> windowingStrategy,
        Comparator<BoundedWindow> windowComparator) {
      return create(toValue, context.getType(windowingStrategy), windowComparator);
    }

    static <InputT, ValueT, AccumT> WindowedAccumulator<InputT, ValueT, AccumT, ?> create(
        Function<InputT, ValueT> toValue, Type type, Comparator<BoundedWindow> windowComparator) {
      switch (type) {
        case MERGING:
          return MergingWindowedAccumulator.create(toValue, windowComparator);
        case NON_MERGING:
          return NonMergingWindowedAccumulator.create(toValue);
        case SINGLE_WINDOW:
        case EXPLODE_WINDOWS:
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
        Comparator<BoundedWindow> windowComparator) {
      switch (type) {
        case MERGING:
          return MergingWindowedAccumulator.from(toValue, values, windowComparator);
        case NON_MERGING:
          return NonMergingWindowedAccumulator.from(toValue, values);
        case SINGLE_WINDOW:
        case EXPLODE_WINDOWS:
          Iterator<WindowedValue<AccumT>> iter = values.iterator();
          if (iter.hasNext()) {
            return SingleWindowWindowedAccumulator.create(toValue, iter.next());
          }
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

    static <InputT, ValueT, AccumT> SingleWindowWindowedAccumulator<InputT, ValueT, AccumT> create(
        Function<InputT, ValueT> toValue, WindowedValue<AccumT> accumulator) {
      return new SingleWindowWindowedAccumulator<>(toValue, accumulator);
    }

    final Function<InputT, ValueT> toValue;
    AccumT windowAccumulator = null;
    Instant accTimestamp = null;
    BoundedWindow accWindow = null;

    SingleWindowWindowedAccumulator(Function<InputT, ValueT> toValue) {
      this.toValue = toValue;
    }

    SingleWindowWindowedAccumulator(
        Function<InputT, ValueT> toValue, WindowedValue<AccumT> accumulator) {
      this.toValue = toValue;
      this.windowAccumulator = accumulator.getValue();
      this.accTimestamp = accumulator.getTimestamp();
      this.accWindow = getWindow(accumulator);
    }

    @Override
    public void add(WindowedValue<InputT> value, SparkCombineFn<InputT, ValueT, AccumT, ?> context)
        throws Exception {
      BoundedWindow window = getWindow(value);
      SparkCombineContext ctx = context.ctxtForValue(value);
      TimestampCombiner combiner = context.windowingStrategy.getTimestampCombiner();
      Instant windowTimestamp = combiner.assign(window, value.getTimestamp());
      final AccumT acc;
      final Instant timestamp;
      if (windowAccumulator == null) {
        acc = context.combineFn.createAccumulator(ctx);
        timestamp = windowTimestamp;
      } else {
        acc = windowAccumulator;
        timestamp = accTimestamp;
      }
      AccumT result = context.combineFn.addInput(acc, toValue(value), ctx);
      Instant timestampCombined = combiner.combine(windowTimestamp, timestamp);
      windowAccumulator = result;
      accTimestamp = timestampCombined;
      accWindow = window;
    }

    @Override
    public void merge(
        SingleWindowWindowedAccumulator<InputT, ValueT, AccumT> other,
        SparkCombineFn<?, ?, AccumT, ?> context) {
      if (windowAccumulator != null && other.windowAccumulator != null) {
        List<AccumT> accumulators = Arrays.asList(windowAccumulator, other.windowAccumulator);
        AccumT merged =
            context.combineFn.mergeAccumulators(
                accumulators, context.ctxtForWindows(Arrays.asList(accWindow)));
        Instant combined =
            context
                .windowingStrategy
                .getTimestampCombiner()
                .combine(accTimestamp, other.accTimestamp);
        windowAccumulator = merged;
        accTimestamp = combined;
      } else if (windowAccumulator == null) {
        windowAccumulator = other.windowAccumulator;
        accTimestamp = other.accTimestamp;
        accWindow = other.accWindow;
      }
    }

    @Override
    public Collection<WindowedValue<AccumT>> extractOutput() {
      if (windowAccumulator != null) {
        return Collections.singletonList(
            WindowedValue.of(
                windowAccumulator, accTimestamp, accWindow, PaneInfo.ON_TIME_AND_ONLY_FIRING));
      }
      return Collections.emptyList();
    }

    private ValueT toValue(WindowedValue<InputT> input) {
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
   * @param <ImplT> type of final subclass
   */
  abstract static class MapBasedWindowedAccumulator<
          InputT,
          ValueT,
          AccumT,
          ImplT extends MapBasedWindowedAccumulator<InputT, ValueT, AccumT, ImplT>>
      implements WindowedAccumulator<InputT, ValueT, AccumT, ImplT> {

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
        SparkCombineContext ctx = context.ctxtForValue(v);
        BoundedWindow window = getWindow(v);
        TimestampCombiner combiner = context.windowingStrategy.getTimestampCombiner();
        Instant windowTimestamp = combiner.assign(window, v.getTimestamp());
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
    public void merge(ImplT other, SparkCombineFn<?, ?, AccumT, ?> context) throws Exception {
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
                          context.ctxtForValue(acc)),
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
        Function<InputT, ValueT> toValue, Comparator<BoundedWindow> windowComparator) {
      return new MergingWindowedAccumulator<>(toValue, windowComparator);
    }

    static <InputT, ValueT, AccumT> MergingWindowedAccumulator<InputT, ValueT, AccumT> from(
        Function<InputT, ValueT> toValue,
        Iterable<WindowedValue<AccumT>> values,
        Comparator<BoundedWindow> windowComparator) {
      return new MergingWindowedAccumulator<>(toValue, values, windowComparator);
    }

    @SuppressWarnings("unchecked")
    private MergingWindowedAccumulator(
        Function<InputT, ValueT> toValue, Comparator<BoundedWindow> windowComparator) {
      super(toValue, new TreeMap<>(windowComparator));
    }

    private MergingWindowedAccumulator(
        Function<InputT, ValueT> toValue,
        Iterable<WindowedValue<AccumT>> values,
        Comparator<BoundedWindow> windowComparator) {
      super(toValue, asMap(values, new TreeMap<>(windowComparator)));
    }

    @Override
    void mergeWindows(SparkCombineFn<?, ?, AccumT, ?> fn) throws Exception {

      SparkCombineContext ctx = fn.ctxtForWindows(this.map.keySet());

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

    private final Function<InputT, ValueT> toValue;
    private final IterableCoder<WindowedValue<AccumT>> wrap;
    private final Coder<WindowedValue<AccumT>> accumCoder;
    private final Comparator<BoundedWindow> windowComparator;
    private final WindowedAccumulator.Type type;

    @SuppressWarnings("unchecked")
    WindowedAccumulatorCoder(
        Function<InputT, ValueT> toValue,
        Coder<BoundedWindow> windowCoder,
        Comparator<BoundedWindow> windowComparator,
        Coder<AccumT> accumCoder,
        WindowedAccumulator.Type type) {

      this.toValue = toValue;
      this.accumCoder = WindowedValue.FullWindowedValueCoder.of(accumCoder, windowCoder);
      this.windowComparator = windowComparator;
      this.wrap = IterableCoder.of(this.accumCoder);
      this.type = type;
    }

    @Override
    public void encode(WindowedAccumulator<InputT, ValueT, AccumT, ?> value, OutputStream outStream)
        throws IOException {

      if (type.isMapBased()) {
        wrap.encode(((MapBasedWindowedAccumulator<?, ?, AccumT, ?>) value).map.values(), outStream);
      } else {
        SingleWindowWindowedAccumulator<?, ?, AccumT> swwa =
            (SingleWindowWindowedAccumulator<?, ?, AccumT>) value;
        if (swwa.isEmpty()) {
          outStream.write(0);
        } else {
          outStream.write(1);
          accumCoder.encode(
              WindowedValue.of(
                  swwa.windowAccumulator, swwa.accTimestamp, swwa.accWindow, PaneInfo.NO_FIRING),
              outStream);
        }
      }
    }

    @Override
    public WindowedAccumulator<InputT, ValueT, AccumT, ?> decode(InputStream inStream)
        throws IOException {

      if (type.isMapBased()) {
        return WindowedAccumulator.create(toValue, type, wrap.decode(inStream), windowComparator);
      }
      boolean empty = inStream.read() == 0;
      if (empty) {
        return WindowedAccumulator.create(toValue, type, windowComparator);
      }
      return WindowedAccumulator.create(
          toValue, type, Arrays.asList(accumCoder.decode(inStream)), windowComparator);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return wrap.getComponents();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  /** An implementation of {@link CombineWithContext.Context} for the SparkRunner. */
  static class SparkCombineContext extends CombineWithContext.Context {
    private final PipelineOptions pipelineOptions;
    private final SideInputReader sideInputReader;

    SparkCombineContext(PipelineOptions pipelineOptions, SideInputReader sideInputReader) {
      this.pipelineOptions = pipelineOptions;
      this.sideInputReader = sideInputReader;
    }

    Collection<? extends BoundedWindow> windows = null;

    SparkCombineContext forInput(final Collection<? extends BoundedWindow> windows) {
      this.windows = Objects.requireNonNull(windows);
      return this;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      // validate element window.
      Preconditions.checkState(
          windows.size() == 1,
          "sideInput can only be called when the main " + "input element is in exactly one window");
      return sideInputReader.get(view, windows.iterator().next());
    }
  }

  @VisibleForTesting
  static <K, V, AccumT, OutputT> SparkCombineFn<KV<K, V>, V, AccumT, OutputT> keyed(
      CombineWithContext.CombineFnWithContext<V, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy,
      WindowedAccumulator.Type nonMergingStrategy) {
    return new SparkCombineFn<>(
        false, KV::getValue, combineFn, options, sideInputs, windowingStrategy, nonMergingStrategy);
  }

  public static <K, V, AccumT, OutputT> SparkCombineFn<KV<K, V>, V, AccumT, OutputT> keyed(
      CombineWithContext.CombineFnWithContext<V, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SparkCombineFn<>(
        false, KV::getValue, combineFn, options, sideInputs, windowingStrategy);
  }

  public static <InputT, AccumT, OutputT> SparkCombineFn<InputT, InputT, AccumT, OutputT> globally(
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new SparkCombineFn<>(true, e -> e, combineFn, options, sideInputs, windowingStrategy);
  }

  private static <T> Map<BoundedWindow, WindowedValue<T>> asMap(
      Iterable<WindowedValue<T>> values, Map<BoundedWindow, WindowedValue<T>> res) {
    for (WindowedValue<T> v : values) {
      res.put(getWindow(v), v);
    }
    return res;
  }

  /**
   * Create comparator for given type descriptor. Note that the returned {@link Comparator} has to
   * be {@link Serializable}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Comparator<BoundedWindow> asWindowComparator(
      @Nullable TypeDescriptor<?> windowType) {
    final Comparator<BoundedWindow> comparator;
    if (windowType != null
        && StreamSupport.stream(windowType.getInterfaces().spliterator(), false)
            .anyMatch(t -> t.isSubtypeOf(TypeDescriptor.of(Comparable.class)))) {
      comparator = (Comparator) Comparator.naturalOrder();
    } else {
      java.util.function.Function<BoundedWindow, Instant> keyExtractor =
          (java.util.function.Function<BoundedWindow, Instant> & Serializable)
              BoundedWindow::maxTimestamp;
      comparator = Comparator.comparing(keyExtractor);
    }
    return comparator;
  }

  private final boolean globalCombine;
  private final SerializablePipelineOptions options;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;
  final WindowingStrategy<?, BoundedWindow> windowingStrategy;

  private final Function<InputT, ValueT> toValue;
  private final WindowedAccumulator.Type defaultNonMergingCombineStrategy;
  private final CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> combineFn;
  private final Comparator<BoundedWindow> windowComparator;

  // each Spark task should get it's own copy of this SparkCombineFn, and since Spark tasks
  // are single-threaded, it is safe to reuse the context.
  // the combine context is not Serializable so we'll use lazy initialization.
  // ** DO NOT attempt to turn this into a Singleton as Spark may run multiple tasks in parallel
  // in the same JVM (Executor). **
  // ** DO NOT use combineContext directly inside this class, use ctxtForValue instead. **
  private transient SparkCombineContext combineContext;

  SparkCombineFn(
      boolean global,
      Function<InputT, ValueT> toValue,
      CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    this(
        global,
        toValue,
        combineFn,
        options,
        sideInputs,
        windowingStrategy,
        WindowedAccumulator.Type.EXPLODE_WINDOWS);
  }

  @VisibleForTesting
  SparkCombineFn(
      boolean global,
      Function<InputT, ValueT> toValue,
      CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy,
      WindowedAccumulator.Type defaultNonMergingCombineStrategy) {

    this.globalCombine = global;
    this.options = options;
    this.sideInputs = sideInputs;
    @SuppressWarnings("unchecked")
    WindowingStrategy<?, BoundedWindow> castStrategy = (WindowingStrategy) windowingStrategy;
    this.windowingStrategy = castStrategy;
    this.toValue = toValue;
    this.defaultNonMergingCombineStrategy = defaultNonMergingCombineStrategy;
    this.combineFn = combineFn;
    @SuppressWarnings("unchecked")
    TypeDescriptor<BoundedWindow> untyped =
        (TypeDescriptor<BoundedWindow>) windowingStrategy.getWindowFn().getWindowTypeDescriptor();
    this.windowComparator = asWindowComparator(untyped);
  }

  /** Create empty combiner. Implements Spark's zeroValue for aggregateFn. */
  WindowedAccumulator<InputT, ValueT, AccumT, ?> createCombiner() {
    return WindowedAccumulator.create(this, toValue, windowingStrategy, windowComparator);
  }

  /**
   * Implements Spark's createCombiner function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  WindowedAccumulator<InputT, ValueT, AccumT, ?> createCombiner(WindowedValue<InputT> value) {
    try {
      WindowedAccumulator<InputT, ValueT, AccumT, ?> accumulator =
          WindowedAccumulator.create(this, toValue, windowingStrategy, windowComparator);
      accumulator.add(value, this);
      return accumulator;
    } catch (RuntimeException ex) {
      throw ex;
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
    } catch (RuntimeException ex) {
      throw ex;
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
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  Iterable<WindowedValue<OutputT>> extractOutput(WindowedAccumulator<?, ?, AccumT, ?> accumulator) {
    return extractOutputStream(accumulator).collect(Collectors.toList());
  }

  /** Extracts the stream of accumulated values. */
  public Stream<WindowedValue<OutputT>> extractOutputStream(
      WindowedAccumulator<?, ?, AccumT, ?> accumulator) {
    return accumulator.extractOutput().stream()
        .filter(Objects::nonNull)
        .map(
            windowAcc ->
                windowAcc.withValue(
                    combineFn.extractOutput(windowAcc.getValue(), ctxtForValue(windowAcc))));
  }

  WindowedAccumulatorCoder<InputT, ValueT, AccumT> accumulatorCoder(
      Coder<BoundedWindow> windowCoder,
      Coder<AccumT> accumulatorCoder,
      WindowingStrategy<?, ?> windowingStrategy) {
    return new WindowedAccumulatorCoder<>(
        toValue, windowCoder, windowComparator, accumulatorCoder, getType(windowingStrategy));
  }

  CombineWithContext.CombineFnWithContext<ValueT, AccumT, OutputT> getCombineFn() {
    return combineFn;
  }

  boolean mustBringWindowToKey() {
    return !getType(windowingStrategy).isMapBased();
  }

  private WindowedAccumulator.Type getType(WindowingStrategy<?, ?> windowingStrategy) {
    if (!windowingStrategy.needsMerge()) {
      if (globalCombine) {
        /* global combine must use map-based accumulator to incorporate multiple windows */
        return WindowedAccumulator.Type.NON_MERGING;
      }
      if (windowingStrategy.getWindowFn().assignsToOneWindow()
          && GroupNonMergingWindowsFunctions.isEligibleForGroupByWindow(windowingStrategy)) {
        return WindowedAccumulator.Type.SINGLE_WINDOW;
      }
      return defaultNonMergingCombineStrategy;
    }
    return WindowedAccumulator.Type.MERGING;
  }

  private static BoundedWindow getWindow(WindowedValue<?> value) {
    if (value.isSingleWindowedValue()) {
      return ((WindowedValue.SingleWindowedValue) value).getWindow();
    }
    return Iterables.getOnlyElement(value.getWindows());
  }

  @SuppressWarnings("unchecked")
  SparkCombineContext ctxtForValue(WindowedValue<?> input) {
    return ctxtForWindows((Collection) input.getWindows());
  }

  SparkCombineContext ctxtForWindows(Collection<BoundedWindow> windows) {
    if (combineContext == null) {
      combineContext = new SparkCombineContext(options.get(), new SparkSideInputReader(sideInputs));
    }
    return combineContext.forInput(windows);
  }
}
