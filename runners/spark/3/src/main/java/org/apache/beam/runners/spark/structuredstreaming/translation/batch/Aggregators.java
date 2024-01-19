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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.collectionEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.mapEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.mutablePairEncoder;
import static org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators.peekingIterator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun1;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.PeekingIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.util.MutablePair;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.joda.time.Instant;

@Internal
class Aggregators {

  /**
   * Creates simple value {@link Aggregator} that is not window aware.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   */
  static <ValT, AccT, ResT, InT> Aggregator<InT, ?, ResT> value(
      CombineFn<ValT, AccT, ResT> fn,
      Fun1<InT, ValT> valueFn,
      Encoder<AccT> accEnc,
      Encoder<ResT> outEnc) {
    return new ValueAggregator<>(fn, valueFn, accEnc, outEnc);
  }

  /**
   * Creates windowed Spark {@link Aggregator} depending on the provided Beam {@link WindowFn}s.
   *
   * <p>Specialised implementations are provided for:
   * <li>{@link Sessions}
   * <li>Non merging window functions
   * <li>Merging window functions
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   */
  static <ValT, AccT, ResT, InT>
      Aggregator<WindowedValue<InT>, ?, Collection<WindowedValue<ResT>>> windowedValue(
          CombineFn<ValT, AccT, ResT> fn,
          Fun1<WindowedValue<InT>, ValT> valueFn,
          WindowingStrategy<?, ?> windowing,
          Encoder<BoundedWindow> windowEnc,
          Encoder<AccT> accEnc,
          Encoder<WindowedValue<ResT>> outEnc) {
    if (!windowing.needsMerge()) {
      return new NonMergingWindowedAggregator<>(fn, valueFn, windowing, windowEnc, accEnc, outEnc);
    } else if (windowing.getWindowFn().getClass().equals(Sessions.class)) {
      return new SessionsAggregator<>(fn, valueFn, windowing, (Encoder) windowEnc, accEnc, outEnc);
    }
    return new MergingWindowedAggregator<>(fn, valueFn, windowing, windowEnc, accEnc, outEnc);
  }

  /**
   * Simple value {@link Aggregator} that is not window aware.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   */
  private static class ValueAggregator<ValT, AccT, ResT, InT>
      extends CombineFnAggregator<ValT, AccT, ResT, InT, AccT, ResT> {

    public ValueAggregator(
        CombineFn<ValT, AccT, ResT> fn,
        Fun1<InT, ValT> valueFn,
        Encoder<AccT> accEnc,
        Encoder<ResT> outEnc) {
      super(fn, valueFn, accEnc, outEnc);
    }

    @Override
    public AccT zero() {
      return emptyAcc();
    }

    @Override
    public AccT reduce(AccT buff, InT in) {
      return addToAcc(buff, value(in));
    }

    @Override
    public AccT merge(AccT b1, AccT b2) {
      return mergeAccs(b1, b2);
    }

    @Override
    public ResT finish(AccT buff) {
      return extract(buff);
    }
  }

  /**
   * Specialized windowed Spark {@link Aggregator} for Beam {@link WindowFn}s of type {@link
   * Sessions}. The aggregator uses a {@link TreeMap} as buffer to maintain ordering of the {@link
   * IntervalWindow}s and merge these more efficiently.
   *
   * <p>For efficiency, this aggregator re-implements {@link
   * Sessions#mergeWindows(WindowFn.MergeContext)} to leverage the already sorted buffer.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   */
  private static class SessionsAggregator<ValT, AccT, ResT, InT>
      extends WindowedAggregator<
          ValT,
          AccT,
          ResT,
          InT,
          IntervalWindow,
          TreeMap<IntervalWindow, MutablePair<Instant, AccT>>> {

    SessionsAggregator(
        CombineFn<ValT, AccT, ResT> combineFn,
        Fun1<WindowedValue<InT>, ValT> valueFn,
        WindowingStrategy<?, ?> windowing,
        Encoder<IntervalWindow> windowEnc,
        Encoder<AccT> accEnc,
        Encoder<WindowedValue<ResT>> outEnc) {
      super(combineFn, valueFn, windowing, windowEnc, accEnc, outEnc, (Class) TreeMap.class);
      checkArgument(windowing.getWindowFn().getClass().equals(Sessions.class));
    }

    @Override
    public final TreeMap<IntervalWindow, MutablePair<Instant, AccT>> zero() {
      return new TreeMap<>();
    }

    @Override
    @SuppressWarnings("keyfor")
    public TreeMap<IntervalWindow, MutablePair<Instant, AccT>> reduce(
        TreeMap<IntervalWindow, MutablePair<Instant, AccT>> buff, WindowedValue<InT> input) {
      for (IntervalWindow window : (Collection<IntervalWindow>) input.getWindows()) {
        @MonotonicNonNull MutablePair<Instant, AccT> acc = null;
        @MonotonicNonNull IntervalWindow first = null, last = null;
        // start with window before or equal to new window (if exists)
        @Nullable Entry<IntervalWindow, MutablePair<Instant, AccT>> lower = buff.floorEntry(window);
        if (lower != null && window.intersects(lower.getKey())) {
          // if intersecting, init accumulator and extend window to span both
          acc = lower.getValue();
          window = window.span(lower.getKey());
          first = last = lower.getKey();
        }
        // merge following windows in order if they intersect, then stop
        for (Entry<IntervalWindow, MutablePair<Instant, AccT>> entry :
            buff.tailMap(window, false).entrySet()) {
          MutablePair<Instant, AccT> entryAcc = entry.getValue();
          IntervalWindow entryWindow = entry.getKey();
          if (window.intersects(entryWindow)) {
            // extend window and merge accumulators
            window = window.span(entryWindow);
            acc = acc == null ? entryAcc : mergeAccs(window, acc, entryAcc);
            if (first == null) {
              // there was no previous (lower) window intersecting the input window
              first = last = entryWindow;
            } else {
              last = entryWindow;
            }
          } else {
            break; // stop, later windows won't intersect either
          }
        }
        if (first != null && last != null) {
          // remove entire subset from from first to last after it got merged into acc
          buff.navigableKeySet().subSet(first, true, last, true).clear();
        }
        // add input and get accumulator for new (potentially merged) window
        buff.put(window, addToAcc(window, acc, value(input), input.getTimestamp()));
      }
      return buff;
    }

    @Override
    public TreeMap<IntervalWindow, MutablePair<Instant, AccT>> merge(
        TreeMap<IntervalWindow, MutablePair<Instant, AccT>> b1,
        TreeMap<IntervalWindow, MutablePair<Instant, AccT>> b2) {
      if (b1.isEmpty()) {
        return b2;
      } else if (b2.isEmpty()) {
        return b1;
      }
      // Init new tree map to merge both buffers
      TreeMap<IntervalWindow, MutablePair<Instant, AccT>> res = zero();
      PeekingIterator<Entry<IntervalWindow, MutablePair<Instant, AccT>>> it1 =
          peekingIterator(b1.entrySet().iterator());
      PeekingIterator<Entry<IntervalWindow, MutablePair<Instant, AccT>>> it2 =
          peekingIterator(b2.entrySet().iterator());

      @Nullable MutablePair<Instant, AccT> acc = null;
      @Nullable IntervalWindow window = null;
      while (it1.hasNext() || it2.hasNext()) {
        // pick iterator with the smallest window ahead and forward it
        Entry<IntervalWindow, MutablePair<Instant, AccT>> nextMin =
            (it1.hasNext() && it2.hasNext())
                ? it1.peek().getKey().compareTo(it2.peek().getKey()) <= 0 ? it1.next() : it2.next()
                : it1.hasNext() ? it1.next() : it2.next();
        if (window != null && window.intersects(nextMin.getKey())) {
          // extend window and merge accumulators if intersecting
          window = window.span(nextMin.getKey());
          acc = mergeAccs(window, acc, nextMin.getValue());
        } else {
          // store window / accumulator if necessary and continue with next minimum
          if (window != null && acc != null) {
            res.put(window, acc);
          }
          acc = nextMin.getValue();
          window = nextMin.getKey();
        }
      }
      if (window != null && acc != null) {
        res.put(window, acc);
      }
      return res;
    }
  }

  /**
   * Merging windowed Spark {@link Aggregator} using a Map of {@link BoundedWindow}s as aggregation
   * buffer. When reducing new input, a windowed accumulator is created for each new window of the
   * input that doesn't overlap with existing windows. Otherwise, if the window is known or
   * overlaps, the window is extended accordingly and accumulators are merged.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   */
  private static class MergingWindowedAggregator<ValT, AccT, ResT, InT>
      extends NonMergingWindowedAggregator<ValT, AccT, ResT, InT> {

    private final WindowFn<ValT, BoundedWindow> windowFn;

    public MergingWindowedAggregator(
        CombineFn<ValT, AccT, ResT> combineFn,
        Fun1<WindowedValue<InT>, ValT> valueFn,
        WindowingStrategy<?, ?> windowing,
        Encoder<BoundedWindow> windowEnc,
        Encoder<AccT> accEnc,
        Encoder<WindowedValue<ResT>> outEnc) {
      super(combineFn, valueFn, windowing, windowEnc, accEnc, outEnc);
      windowFn = (WindowFn<ValT, BoundedWindow>) windowing.getWindowFn();
    }

    @Override
    protected Map<BoundedWindow, MutablePair<Instant, AccT>> reduce(
        Map<BoundedWindow, MutablePair<Instant, AccT>> buff,
        Collection<BoundedWindow> windows,
        ValT value,
        Instant timestamp) {
      if (buff.isEmpty()) {
        // no windows yet to be merged, use the non-merging behavior of super
        return super.reduce(buff, windows, value, timestamp);
      }
      // Merge multiple windows into one target window using the reducer function if the window
      // already exists. Otherwise, the input value is added to the accumulator. Merged windows are
      // removed from the accumulator map.
      Function<BoundedWindow, ReduceFn<AccT>> accFn =
          target ->
              (acc, w) -> {
                MutablePair<Instant, AccT> accW = buff.remove(w);
                return (accW != null)
                    ? mergeAccs(w, acc, accW)
                    : addToAcc(w, acc, value, timestamp);
              };
      Set<BoundedWindow> unmerged = mergeWindows(buff, ImmutableSet.copyOf(windows), accFn);
      if (!unmerged.isEmpty()) {
        // remaining windows don't have to be merged
        return super.reduce(buff, unmerged, value, timestamp);
      }
      return buff;
    }

    @Override
    public Map<BoundedWindow, MutablePair<Instant, AccT>> merge(
        Map<BoundedWindow, MutablePair<Instant, AccT>> b1,
        Map<BoundedWindow, MutablePair<Instant, AccT>> b2) {
      // Merge multiple windows into one target window using the reducer function. Merged windows
      // are removed from both accumulator maps
      Function<BoundedWindow, ReduceFn<AccT>> reduceFn =
          target -> (acc, w) -> mergeAccs(w, mergeAccs(w, acc, b1.remove(w)), b2.remove(w));

      Set<BoundedWindow> unmerged = b2.keySet();
      unmerged = mergeWindows(b1, unmerged, reduceFn);
      if (!unmerged.isEmpty()) {
        // keep only unmerged windows in 2nd accumulator map, continue using "non-merging" merge
        b2.keySet().retainAll(unmerged);
        return super.merge(b1, b2);
      }
      return b1;
    }

    /** Reduce function to merge multiple windowed accumulator values into one target window. */
    private interface ReduceFn<AccT>
        extends BiFunction<
            @Nullable MutablePair<Instant, AccT>,
            BoundedWindow,
            @Nullable MutablePair<Instant, AccT>> {}

    /**
     * Attempt to merge windows of accumulator map with additional windows using the reducer
     * function. The reducer function must support {@code null} as zero value.
     *
     * @return The subset of additional windows that don't require a merge.
     */
    private Set<BoundedWindow> mergeWindows(
        Map<BoundedWindow, MutablePair<Instant, AccT>> buff,
        Set<BoundedWindow> newWindows,
        Function<BoundedWindow, ReduceFn<AccT>> reduceFn) {
      try {
        Set<BoundedWindow> newUnmerged = new HashSet<>(newWindows);
        windowFn.mergeWindows(
            windowFn.new MergeContext() {
              @Override
              public Collection<BoundedWindow> windows() {
                return Sets.union(buff.keySet(), newWindows);
              }

              @Override
              public void merge(Collection<BoundedWindow> merges, BoundedWindow target) {
                @Nullable
                MutablePair<Instant, AccT> merged =
                    merges.stream().reduce(null, reduceFn.apply(target), combiner(target));
                if (merged != null) {
                  buff.put(target, merged);
                }
                newUnmerged.removeAll(merges);
              }
            });
        return newUnmerged;
      } catch (Exception e) {
        throw new RuntimeException("Unable to merge accumulators windows", e);
      }
    }
  }

  /**
   * Non-merging windowed Spark {@link Aggregator} using a Map of {@link BoundedWindow}s as
   * aggregation buffer. When reducing new input, a windowed accumulator is created for each new
   * window of the input. Otherwise, if the window is known, the accumulators are merged.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   */
  private static class NonMergingWindowedAggregator<ValT, AccT, ResT, InT>
      extends WindowedAggregator<
          ValT, AccT, ResT, InT, BoundedWindow, Map<BoundedWindow, MutablePair<Instant, AccT>>> {

    public NonMergingWindowedAggregator(
        CombineFn<ValT, AccT, ResT> combineFn,
        Fun1<WindowedValue<InT>, ValT> valueFn,
        WindowingStrategy<?, ?> windowing,
        Encoder<BoundedWindow> windowEnc,
        Encoder<AccT> accEnc,
        Encoder<WindowedValue<ResT>> outEnc) {
      super(combineFn, valueFn, windowing, windowEnc, accEnc, outEnc, (Class) Map.class);
    }

    @Override
    public Map<BoundedWindow, MutablePair<Instant, AccT>> zero() {
      return new HashMap<>();
    }

    @Override
    public final Map<BoundedWindow, MutablePair<Instant, AccT>> reduce(
        Map<BoundedWindow, MutablePair<Instant, AccT>> buff, WindowedValue<InT> input) {
      Collection<BoundedWindow> windows = (Collection<BoundedWindow>) input.getWindows();
      return reduce(buff, windows, value(input), input.getTimestamp());
    }

    protected Map<BoundedWindow, MutablePair<Instant, AccT>> reduce(
        Map<BoundedWindow, MutablePair<Instant, AccT>> buff,
        Collection<BoundedWindow> windows,
        ValT value,
        Instant timestamp) {
      // for each window add the value to the accumulator
      for (BoundedWindow window : windows) {
        buff.compute(window, (w, acc) -> addToAcc(w, acc, value, timestamp));
      }
      return buff;
    }

    @Override
    public Map<BoundedWindow, MutablePair<Instant, AccT>> merge(
        Map<BoundedWindow, MutablePair<Instant, AccT>> b1,
        Map<BoundedWindow, MutablePair<Instant, AccT>> b2) {
      if (b1.isEmpty()) {
        return b2;
      } else if (b2.isEmpty()) {
        return b1;
      }
      if (b2.size() > b1.size()) {
        return merge(b2, b1);
      }
      // merge entries of (smaller) 2nd agg buffer map into first by merging the accumulators
      b2.forEach((w, acc) -> b1.merge(w, acc, combiner(w)));
      return b1;
    }
  }

  /**
   * Abstract base of a Spark {@link Aggregator} on {@link WindowedValue}s using a Map of {@link W}
   * as aggregation buffer.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} / {@link Aggregator} result type
   * @param <InT> {@link Aggregator} input type
   * @param <W> bounded window type
   * @param <MapT> aggregation buffer {@link W}
   */
  private abstract static class WindowedAggregator<
          ValT,
          AccT,
          ResT,
          InT,
          W extends @NonNull BoundedWindow,
          MapT extends Map<W, @NonNull MutablePair<Instant, AccT>>>
      extends CombineFnAggregator<
          ValT, AccT, ResT, WindowedValue<InT>, MapT, Collection<WindowedValue<ResT>>> {
    private final TimestampCombiner tsCombiner;

    public WindowedAggregator(
        CombineFn<ValT, AccT, ResT> combineFn,
        Fun1<WindowedValue<InT>, ValT> valueFn,
        WindowingStrategy<?, ?> windowing,
        Encoder<W> windowEnc,
        Encoder<AccT> accEnc,
        Encoder<WindowedValue<ResT>> outEnc,
        Class<MapT> clazz) {
      super(
          combineFn,
          valueFn,
          mapEncoder(windowEnc, mutablePairEncoder(encoderOf(Instant.class), accEnc), clazz),
          collectionEncoder(outEnc));
      tsCombiner = windowing.getTimestampCombiner();
    }

    protected final Instant resolveTimestamp(BoundedWindow w, Instant t1, Instant t2) {
      return tsCombiner.merge(w, t1, t2);
    }

    /** Init accumulator with initial input value and timestamp. */
    protected final MutablePair<Instant, AccT> initAcc(ValT value, Instant timestamp) {
      return new MutablePair<>(timestamp, addToAcc(emptyAcc(), value));
    }

    /** Merge timestamped accumulators. */
    protected final <T extends MutablePair<Instant, AccT>> @PolyNull T mergeAccs(
        W window, @PolyNull T a1, @PolyNull T a2) {
      if (a1 == null || a2 == null) {
        return a1 == null ? a2 : a1;
      }
      return (T) a1.update(resolveTimestamp(window, a1._1, a2._1), mergeAccs(a1._2, a2._2));
    }

    protected BinaryOperator<@Nullable MutablePair<Instant, AccT>> combiner(W target) {
      return (a1, a2) -> mergeAccs(target, a1, a2);
    }

    /** Add an input value to a nullable accumulator. */
    protected final MutablePair<Instant, AccT> addToAcc(
        W window, @Nullable MutablePair<Instant, AccT> acc, ValT val, Instant ts) {
      if (acc == null) {
        return initAcc(val, ts);
      }
      return acc.update(resolveTimestamp(window, acc._1, ts), addToAcc(acc._2, val));
    }

    @Override
    @SuppressWarnings("nullness") // entries are non null
    public final Collection<WindowedValue<ResT>> finish(MapT buffer) {
      return Collections2.transform(buffer.entrySet(), this::windowedValue);
    }

    private WindowedValue<ResT> windowedValue(Entry<W, MutablePair<Instant, AccT>> e) {
      return WindowedValue.of(extract(e.getValue()._2), e.getValue()._1, e.getKey(), NO_FIRING);
    }
  }

  /**
   * Abstract base of Spark {@link Aggregator}s using a Beam {@link CombineFn}.
   *
   * @param <ValT> {@link CombineFn} input type
   * @param <AccT> {@link CombineFn} accumulator type
   * @param <ResT> {@link CombineFn} result type
   * @param <InT> {@link Aggregator} input type
   * @param <BuffT> {@link Aggregator} buffer type
   * @param <OutT> {@link Aggregator} output type
   */
  private abstract static class CombineFnAggregator<ValT, AccT, ResT, InT, BuffT, OutT>
      extends Aggregator<InT, BuffT, OutT> {
    private final CombineFn<ValT, AccT, ResT> fn;
    private final Fun1<InT, ValT> valueFn;
    private final Encoder<BuffT> bufferEnc;
    private final Encoder<OutT> outputEnc;

    public CombineFnAggregator(
        CombineFn<ValT, AccT, ResT> fn,
        Fun1<InT, ValT> valueFn,
        Encoder<BuffT> bufferEnc,
        Encoder<OutT> outputEnc) {
      this.fn = fn;
      this.valueFn = valueFn;
      this.bufferEnc = bufferEnc;
      this.outputEnc = outputEnc;
    }

    protected final ValT value(InT in) {
      return valueFn.apply(in);
    }

    protected final AccT emptyAcc() {
      return fn.createAccumulator();
    }

    protected final AccT mergeAccs(AccT a1, AccT a2) {
      return fn.mergeAccumulators(ImmutableList.of(a1, a2));
    }

    protected final AccT addToAcc(AccT acc, ValT val) {
      return fn.addInput(acc, val);
    }

    protected final ResT extract(AccT acc) {
      return fn.extractOutput(acc);
    }

    @Override
    public Encoder<BuffT> bufferEncoder() {
      return bufferEnc;
    }

    @Override
    public Encoder<OutT> outputEncoder() {
      return outputEnc;
    }
  }
}
