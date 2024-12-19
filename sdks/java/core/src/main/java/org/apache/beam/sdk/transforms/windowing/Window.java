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
package org.apache.beam.sdk.transforms.windowing;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.KeyedWindow.KeyedWindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * {@link Window} logically divides up or groups the elements of a {@link PCollection} into finite
 * windows according to a {@link WindowFn}. The output of {@code Window} contains the same elements
 * as input, but they have been logically assigned to windows. The next {@link
 * org.apache.beam.sdk.transforms.GroupByKey GroupByKeys}, including one within composite
 * transforms, will group by the combination of keys and windows.
 *
 * <p>See {@link org.apache.beam.sdk.transforms.GroupByKey} for more information about how grouping
 * with windows works.
 *
 * <h2>Windowing</h2>
 *
 * <p>Windowing a {@link PCollection} divides the elements into windows based on the associated
 * event time for each element. This is especially useful for {@link PCollection PCollections} with
 * unbounded size, since it allows operating on a sub-group of the elements placed into a related
 * window. For {@link PCollection PCollections} with a bounded size (aka. conventional batch mode),
 * by default, all data is implicitly in a single window, unless {@link Window} is applied.
 *
 * <p>For example, a simple form of windowing divides up the data into fixed-width time intervals,
 * using {@link FixedWindows}. The following example demonstrates how to use {@link Window} in a
 * pipeline that counts the number of occurrences of strings each minute:
 *
 * <pre>{@code
 * PCollection<String> items = ...;
 * PCollection<String> windowed_items = items.apply(
 *   Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
 * PCollection<KV<String, Long>> windowed_counts = windowed_items.apply(
 *   Count.<String>perElement());
 * }</pre>
 *
 * <p>Let (data, timestamp) denote a data element along with its timestamp. Then, if the input to
 * this pipeline consists of {("foo", 15s), ("bar", 30s), ("foo", 45s), ("foo", 1m30s)}, the output
 * will be {(KV("foo", 2), 1m), (KV("bar", 1), 1m), (KV("foo", 1), 2m)}
 *
 * <p>Several predefined {@link WindowFn}s are provided:
 *
 * <ul>
 *   <li>{@link FixedWindows} partitions the timestamps into fixed-width intervals.
 *   <li>{@link SlidingWindows} places data into overlapping fixed-width intervals.
 *   <li>{@link Sessions} groups data into sessions where each item in a window is separated from
 *       the next by no more than a specified gap.
 * </ul>
 *
 * <p>Additionally, custom {@link WindowFn}s can be created, by creating new subclasses of {@link
 * WindowFn}.
 *
 * <h2>Triggers</h2>
 *
 * <p>{@link Window#triggering(Trigger)} allows specifying a trigger to control when (in processing
 * time) results for the given window can be produced. If unspecified, the default behavior is to
 * trigger first when the watermark passes the end of the window, and then trigger again every time
 * there is late arriving data.
 *
 * <p>Elements are added to the current window pane as they arrive. When the root trigger fires,
 * output is produced based on the elements in the current pane.
 *
 * <p>Depending on the trigger, this can be used both to output partial results early during the
 * processing of the whole window, and to deal with late arriving in batches.
 *
 * <p>Continuing the earlier example, if we wanted to emit the values that were available when the
 * watermark passed the end of the window, and then output any late arriving elements once-per
 * (actual hour) hour until we have finished processing the next 24-hours of data. (The use of
 * watermark time to stop processing tends to be more robust if the data source is slow for a few
 * days, etc.)
 *
 * <pre>{@code
 * PCollection<String> items = ...;
 * PCollection<String> windowed_items = items.apply(
 *   Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
 *      .triggering(
 *          AfterWatermark.pastEndOfWindow()
 *              .withLateFirings(AfterProcessingTime
 *                  .pastFirstElementInPane().plusDelayOf(Duration.standardHours(1))))
 *      .withAllowedLateness(Duration.standardDays(1)));
 * PCollection<KV<String, Long>> windowed_counts = windowed_items.apply(
 *   Count.<String>perElement());
 * }</pre>
 *
 * <p>On the other hand, if we wanted to get early results every minute of processing time (for
 * which there were new elements in the given window) we could do the following:
 *
 * <pre>{@code
 * PCollection<String> windowed_items = items.apply(
 *   Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
 *      .triggering(
 *          AfterWatermark.pastEndOfWindow()
 *              .withEarlyFirings(AfterProcessingTime
 *                  .pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
 *      .withAllowedLateness(Duration.ZERO));
 * }</pre>
 *
 * <p>After a {@link org.apache.beam.sdk.transforms.GroupByKey} the trigger is set to a trigger that
 * will preserve the intent of the upstream trigger. See {@link Trigger#getContinuationTrigger} for
 * more information.
 *
 * <p>See {@link Trigger} for details on the available triggers.
 */
@AutoValue
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public abstract class Window<T> extends PTransform<PCollection<T>, PCollection<T>> {

  /**
   * Specifies the conditions under which a final pane will be created when a window is permanently
   * closed.
   */
  public enum ClosingBehavior {
    /**
     * Always fire the last pane. Even if there is no new data since the previous firing, an element
     * with {@link PaneInfo#isLast()} {@code true} will be produced.
     */
    FIRE_ALWAYS,
    /**
     * Only fire the last pane if there is new data since the previous firing.
     *
     * <p>This is the default behavior.
     */
    FIRE_IF_NON_EMPTY
  }

  /**
   * Specifies the conditions under which an on-time pane will be created when a window is closed.
   */
  public enum OnTimeBehavior {
    /**
     * Always fire the on-time pane. Even if there is no new data since the previous firing, an
     * element will be produced.
     *
     * <p>This is the default behavior.
     */
    FIRE_ALWAYS,
    /** Only fire the on-time pane if there is new data since the previous firing. */
    FIRE_IF_NON_EMPTY
  }

  /**
   * Creates a {@code Window} {@code PTransform} that uses the given {@link WindowFn} to window the
   * data.
   *
   * <p>The resulting {@code PTransform}'s types have been bound, with both the input and output
   * being a {@code PCollection<T>}, inferred from the types of the argument {@code WindowFn}. It is
   * ready to be applied, or further properties can be set on it first.
   */
  public static <T> Window<T> into(WindowFn<? super T, ?> fn) {
    try {
      fn.windowCoder().verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalArgumentException("Window coders must be deterministic.", e);
    }
    return Window.<T>configure().withWindowFn(fn);
  }

  /**
   * Returns a new builder for a {@link Window} transform for setting windowing parameters other
   * than the windowing function.
   */
  public static <T> Window<T> configure() {
    return new AutoValue_Window.Builder<T>().build();
  }

  public abstract @Nullable WindowFn<? super T, ?> getWindowFn();

  abstract @Nullable Trigger getTrigger();

  abstract @Nullable AccumulationMode getAccumulationMode();

  abstract @Nullable Duration getAllowedLateness();

  abstract @Nullable ClosingBehavior getClosingBehavior();

  abstract @Nullable OnTimeBehavior getOnTimeBehavior();

  abstract @Nullable TimestampCombiner getTimestampCombiner();

  abstract Builder<T> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<T> {

    abstract Builder<T> setWindowFn(WindowFn<? super T, ?> windowFn);

    abstract Builder<T> setTrigger(Trigger trigger);

    abstract Builder<T> setAccumulationMode(AccumulationMode mode);

    abstract Builder<T> setAllowedLateness(Duration allowedLateness);

    abstract Builder<T> setClosingBehavior(ClosingBehavior closingBehavior);

    abstract Builder<T> setOnTimeBehavior(OnTimeBehavior onTimeBehavior);

    abstract Builder<T> setTimestampCombiner(TimestampCombiner timestampCombiner);

    abstract Window<T> build();
  }

  private Window<T> withWindowFn(WindowFn<? super T, ?> windowFn) {
    return toBuilder().setWindowFn(windowFn).build();
  }

  /**
   * Sets a non-default trigger for this {@code Window} {@code PTransform}. Elements that are
   * assigned to a specific window will be output when the trigger fires.
   *
   * <p>{@link org.apache.beam.sdk.transforms.windowing.Trigger} has more details on the available
   * triggers.
   *
   * <p>Must also specify allowed lateness using {@link #withAllowedLateness} and accumulation mode
   * using either {@link #discardingFiredPanes()} or {@link #accumulatingFiredPanes()}.
   */
  public Window<T> triggering(Trigger trigger) {
    return toBuilder().setTrigger(trigger).build();
  }

  /**
   * Returns a new {@code Window} {@code PTransform} that uses the registered WindowFn and
   * Triggering behavior, and that discards elements in a pane after they are triggered.
   *
   * <p>Does not modify this transform. The resulting {@code PTransform} is sufficiently specified
   * to be applied, but more properties can still be specified.
   */
  public Window<T> discardingFiredPanes() {
    return toBuilder().setAccumulationMode(AccumulationMode.DISCARDING_FIRED_PANES).build();
  }

  /**
   * Returns a new {@code Window} {@code PTransform} that uses the registered WindowFn and
   * Triggering behavior, and that accumulates elements in a pane after they are triggered.
   *
   * <p>Does not modify this transform. The resulting {@code PTransform} is sufficiently specified
   * to be applied, but more properties can still be specified.
   */
  public Window<T> accumulatingFiredPanes() {
    return toBuilder().setAccumulationMode(AccumulationMode.ACCUMULATING_FIRED_PANES).build();
  }

  /**
   * Override the amount of lateness allowed for data elements in the output {@link PCollection} and
   * downstream {@link PCollection PCollections} until explicitly set again. Like the other
   * properties on this {@link Window} operation, this will be applied at the next {@link
   * GroupByKey}. Any elements that are later than this as decided by the system-maintained
   * watermark will be dropped.
   *
   * <p>This value also determines how long state will be kept around for old windows. Once no
   * elements will be added to a window (because this duration has passed) any state associated with
   * the window will be cleaned up.
   *
   * <p>Depending on the trigger this may not produce a pane with {@link PaneInfo#isLast}. See
   * {@link ClosingBehavior#FIRE_IF_NON_EMPTY} for more details.
   */
  public Window<T> withAllowedLateness(Duration allowedLateness) {
    return toBuilder().setAllowedLateness(allowedLateness).build();
  }

  /**
   * Override the default {@link TimestampCombiner}, to control the output timestamp of values
   * output from a {@link GroupByKey} operation.
   */
  public Window<T> withTimestampCombiner(TimestampCombiner timestampCombiner) {
    return toBuilder().setTimestampCombiner(timestampCombiner).build();
  }

  /**
   * Override the amount of lateness allowed for data elements in the pipeline. Like the other
   * properties on this {@link Window} operation, this will be applied at the next {@link
   * GroupByKey}. Any elements that are later than this as decided by the system-maintained
   * watermark will be dropped.
   *
   * <p>This value also determines how long state will be kept around for old windows. Once no
   * elements will be added to a window (because this duration has passed) any state associated with
   * the window will be cleaned up.
   */
  public Window<T> withAllowedLateness(Duration allowedLateness, ClosingBehavior behavior) {
    return toBuilder().setAllowedLateness(allowedLateness).setClosingBehavior(behavior).build();
  }

  /**
   * Override the default {@link OnTimeBehavior}, to control whether to output an empty on-time
   * pane.
   */
  public Window<T> withOnTimeBehavior(OnTimeBehavior behavior) {
    return toBuilder().setOnTimeBehavior(behavior).build();
  }

  /** Get the output strategy of this {@link Window Window PTransform}. For internal use only. */
  public WindowingStrategy<?, ?> getOutputStrategyInternal(WindowingStrategy<?, ?> inputStrategy) {
    WindowingStrategy<?, ?> result = inputStrategy;
    if (getWindowFn() != null) {
      result = result.withAlreadyMerged(false).withWindowFn(getWindowFn());
    }
    if (getTrigger() != null) {
      result = result.withTrigger(getTrigger());
    }
    if (getAccumulationMode() != null) {
      result = result.withMode(getAccumulationMode());
    }
    if (getAllowedLateness() != null) {
      result =
          result.withAllowedLateness(
              Ordering.natural().max(getAllowedLateness(), inputStrategy.getAllowedLateness()));
    }
    if (getClosingBehavior() != null) {
      result = result.withClosingBehavior(getClosingBehavior());
    }
    if (getOnTimeBehavior() != null) {
      result = result.withOnTimeBehavior(getOnTimeBehavior());
    }
    if (getTimestampCombiner() != null) {
      result = result.withTimestampCombiner(getTimestampCombiner());
    }
    return result;
  }

  private void applicableTo(PCollection<?> input) {
    WindowingStrategy<?, ?> outputStrategy =
        getOutputStrategyInternal(input.getWindowingStrategy());

    boolean isGlobalWindow = (outputStrategy.getWindowFn() instanceof GlobalWindows);
    if (outputStrategy.getWindowFn() instanceof KeyedWindow.KeyedWindowFn) {
      isGlobalWindow =
          (((KeyedWindowFn<?, ?, ?>) outputStrategy.getWindowFn()).getInnerWindowFn()
              instanceof GlobalWindows);
    }

    // Make sure that the windowing strategy is complete & valid.
    if (outputStrategy.isTriggerSpecified()
        && !(outputStrategy.getTrigger() instanceof DefaultTrigger)
        && !(isGlobalWindow)
        && !outputStrategy.isAllowedLatenessSpecified()) {
      throw new IllegalArgumentException(
          "Except when using GlobalWindows,"
              + " calling .triggering() to specify a trigger requires that the allowed lateness"
              + " be specified using .withAllowedLateness() to set the upper bound on how late"
              + " data can arrive before being dropped. See Javadoc for more details.");
    }

    if (!outputStrategy.isModeSpecified() && canProduceMultiplePanes(outputStrategy)) {
      throw new IllegalArgumentException(
          "Calling .triggering() to specify a trigger or calling .withAllowedLateness() to"
              + " specify an allowed lateness greater than zero requires that the accumulation"
              + " mode be specified using .discardingFiredPanes() or .accumulatingFiredPanes()."
              + " See Javadoc for more details.");
    }
  }

  private boolean canProduceMultiplePanes(WindowingStrategy<?, ?> strategy) {
    // The default trigger is Repeatedly.forever(AfterWatermark.pastEndOfWindow()); This fires
    // for every late-arriving element if allowed lateness is nonzero, and thus we must have
    // an accumulating mode specified
    boolean dataCanArriveLate =
        !(strategy.getWindowFn() instanceof GlobalWindows)
            && strategy.getAllowedLateness().getMillis() > 0;
    boolean hasCustomTrigger = !(strategy.getTrigger() instanceof DefaultTrigger);
    return dataCanArriveLate || hasCustomTrigger;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    applicableTo(input);

    WindowingStrategy<?, ?> outputStrategy =
        getOutputStrategyInternal(input.getWindowingStrategy());

    if (getWindowFn() == null) {
      // A new PCollection must be created in case input is reused in a different location as the
      // two PCollections will, in general, have a different windowing strategy.
      return PCollectionList.of(input)
          .apply(Flatten.pCollections())
          .setWindowingStrategyInternal(outputStrategy);
    } else {
      // This is the AssignWindows primitive
      return input.apply(new Assign<>(this, outputStrategy));
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    if (getWindowFn() != null) {
      builder
          .add(
              DisplayData.item("windowFn", getWindowFn().getClass())
                  .withLabel("Windowing Function"))
          .include("windowFn", getWindowFn());
    }

    if (getAllowedLateness() != null) {
      builder.addIfNotDefault(
          DisplayData.item("allowedLateness", getAllowedLateness()).withLabel("Allowed Lateness"),
          Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    }

    if (getTrigger() != null && !(getTrigger() instanceof DefaultTrigger)) {
      builder.add(DisplayData.item("trigger", getTrigger().toString()).withLabel("Trigger"));
    }

    if (getAccumulationMode() != null) {
      builder.add(
          DisplayData.item("accumulationMode", getAccumulationMode().toString())
              .withLabel("Accumulation Mode"));
    }

    if (getClosingBehavior() != null) {
      builder.add(
          DisplayData.item("closingBehavior", getClosingBehavior().toString())
              .withLabel("Window Closing Behavior"));
    }

    if (getTimestampCombiner() != null) {
      builder.add(
          DisplayData.item("timestampCombiner", getTimestampCombiner().toString())
              .withLabel("Timestamp Combiner"));
    }
  }

  @Override
  protected String getKindString() {
    return "Window.Into()";
  }

  /**
   * A Primitive {@link PTransform} that assigns windows to elements based on a {@link WindowFn}.
   * Pipeline authors should use {@link Window} directly instead.
   */
  public static class Assign<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final @Nullable Window<T> original;
    private final WindowingStrategy<T, ?> updatedStrategy;

    /**
     * Create a new {@link Assign} where the output is windowed with the updated {@link
     * WindowingStrategy}. Windows should be assigned using the {@link WindowFn} returned by {@link
     * #getWindowFn()}.
     */
    @VisibleForTesting
    Assign(@Nullable Window<T> original, WindowingStrategy updatedStrategy) {
      this.original = original;
      this.updatedStrategy = updatedStrategy;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), updatedStrategy, input.isBounded(), input.getCoder());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      if (original != null) {
        original.populateDisplayData(builder);
      }
    }

    public @Nullable WindowFn<T, ?> getWindowFn() {
      return updatedStrategy.getWindowFn();
    }

    public static <T> Assign<T> createInternal(WindowingStrategy finalStrategy) {
      return new Assign<T>(null, finalStrategy);
    }
  }

  /**
   * Creates a {@code Window} {@code PTransform} that does not change assigned windows, but will
   * cause windows to be merged again as part of the next {@link
   * org.apache.beam.sdk.transforms.GroupByKey}.
   */
  public static <T> Remerge<T> remerge() {
    return new Remerge<>();
  }

  /**
   * {@code PTransform} that does not change assigned windows, but will cause windows to be merged
   * again as part of the next {@link org.apache.beam.sdk.transforms.GroupByKey}.
   */
  private static class Remerge<T> extends PTransform<PCollection<T>, PCollection<T>> {

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          // We first apply a (trivial) transform to the input PCollection to produce a new
          // PCollection. This ensures that we don't modify the windowing strategy of the input
          // which may be used elsewhere.
          .apply(
              "Identity",
              MapElements.via(
                  new SimpleFunction<T, T>() {
                    @Override
                    public T apply(T element) {
                      return element;
                    }
                  }))
          // Then we modify the windowing strategy.
          .setWindowingStrategyInternal(input.getWindowingStrategy().withAlreadyMerged(false));
    }
  }
}
