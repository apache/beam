/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerId;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Test utility that runs a {@link WindowFn}, {@link Trigger} and {@link AbstractWindowSet} using
 * stub implementations of everything under the hood.
 *
 * <p>To have all interactions between the trigger and underlying components logged, call
 * {@link #logInteractions(boolean)}.
 *
 * @param <InputT> The element types.
 * @param <OutputT> The final type for elements in the window (for instance,
 *     {@code Iterable<InputT>})
 * @param <W> The type of windows being used.
 */
public class TriggerTester<InputT, OutputT, W extends BoundedWindow> {

  private static final Logger LOGGER = Logger.getLogger(TriggerTester.class.getName());

  private Instant watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private final BatchTimerManager timerManager = new LoggingBatchTimerManager(processingTime);
  private final TriggerExecutor<String, InputT, OutputT, W> triggerExecutor;
  private final WindowFn<Object, W> windowFn;
  private final StubContexts stubContexts;
  private final AbstractWindowSet<String, InputT, OutputT, W> windowSet;
  private final Coder<OutputT> outputCoder;

  private static final String KEY = "TEST_KEY";
  private boolean logInteractions = false;
  private ExecutableTrigger<W> executableTrigger;

  private void logInteraction(String fmt, Object... args) {
    if (logInteractions) {
      LOGGER.warning("Trigger Interaction: " + String.format(fmt, args));
    }
  }

  public static <W extends BoundedWindow> TriggerTester<Integer, Iterable<Integer>, W> nonCombining(
      WindowFn<?, W> windowFn, Trigger<W> trigger, AccumulationMode mode) throws Exception {
    @SuppressWarnings("unchecked")
    WindowFn<Object, W> objectWindowFn = (WindowFn<Object, W>) windowFn;

    WindowingStrategy<?, W> strategy =
        WindowingStrategy.of(windowFn).withTrigger(trigger).withMode(mode);
    AbstractWindowSet.Factory<String, Integer, Iterable<Integer>, W> windowSetFactory =
        AbstractWindowSet.<String, Integer, W>factoryFor(strategy, VarIntCoder.of());

    return new TriggerTester<Integer, Iterable<Integer>, W>(
        objectWindowFn, trigger, windowSetFactory, mode, IterableCoder.of(VarIntCoder.of()));
  }

  public static <W extends BoundedWindow, AccumT, OutputT>
      TriggerTester<Integer, OutputT, W> combining(
          WindowFn<?, W> windowFn, Trigger<W> trigger, AccumulationMode mode,
          KeyedCombineFn<String, Integer, AccumT, OutputT> combineFn,
          Coder<OutputT> outputCoder) throws Exception {
    @SuppressWarnings("unchecked")
    WindowFn<Object, W> objectWindowFn = (WindowFn<Object, W>) windowFn;

    AbstractWindowSet.Factory<String, Integer, OutputT, W> windowSetFactory =
        CombiningWindowSet.<String, Integer, AccumT, OutputT, W>factory(
            combineFn, StringUtf8Coder.of(), VarIntCoder.of());

    return new TriggerTester<Integer, OutputT, W>(
        objectWindowFn, trigger, windowSetFactory, mode, outputCoder);
  }

  private TriggerTester(
      WindowFn<Object, W> windowFn,
      Trigger<W> trigger,
      AbstractWindowSet.Factory<String, InputT, OutputT, W> windowSetFactory,
      AccumulationMode mode,
      Coder<OutputT> outputCoder) throws Exception {
    this.windowFn = windowFn;
    this.stubContexts = new StubContexts();
    this.windowSet = windowSetFactory.create(
        KEY, windowFn.windowCoder(), stubContexts, stubContexts);
    this.outputCoder = outputCoder;
    executableTrigger = ExecutableTrigger.create(trigger);
    this.triggerExecutor = new TriggerExecutor<>(
        windowFn, timerManager, executableTrigger, stubContexts, stubContexts, windowSet, mode);
  }

  public ExecutableTrigger<W> getTrigger() {
    return executableTrigger;
  }

  public void logInteractions(boolean logInteractions) {
    this.logInteractions = logInteractions;
  }

  public boolean isDone(W window) throws IOException {
    return triggerExecutor.lookupFinishedSet(window).get(0);
  }

  /**
   * Retrieve the tags of keyed state that is currently stored.
   */
  public Iterable<String> getKeyedStateInUse() {
    return stubContexts.getKeyedStateInUse();
  }

  public String finishedSet(W window) throws CoderException {
    return triggerExecutor.finishedSetTag(window).getId();
  }

  public String bufferTag(W window) throws IOException {
    // We only care about the resulting tag ID, so we don't care about getting the type right.
    return WindowUtils.bufferTag(window, windowFn.windowCoder(), VoidCoder.of()).getId();
  }

  public String earliestElement(W window) throws CoderException {
    return triggerExecutor.earliestElementTag(window).getId();
  }

  public boolean isWindowActive(W window) throws IOException {
    return stubContexts.getKeyedStateInUse()
        .contains(WindowUtils.bufferTag(window, windowFn.windowCoder(), VarIntCoder.of()).getId());
  }

  /**
   * Retrieve the values that have been output to this time, and clear out the output accumulator.
   */
  public Iterable<WindowedValue<OutputT>> extractOutput() {
    ImmutableList<WindowedValue<OutputT>> result = FluentIterable.from(stubContexts.outputs)
        .transform(new Function<WindowedValue<KV<String, OutputT>>, WindowedValue<OutputT>>() {
          @Override
          @Nullable
          public WindowedValue<OutputT> apply(@Nullable WindowedValue<KV<String, OutputT>> input) {
            return WindowedValue.of(
                input.getValue().getValue(), input.getTimestamp(), input.getWindows());
          }
        })
        .toList();
    stubContexts.outputs.clear();
    return result;
  }

  /** Advance the watermark to the specified time, firing any timers that should fire. */
  public void advanceWatermark(Instant newWatermark) throws Exception {
    Preconditions.checkState(!newWatermark.isBefore(watermark),
        "Cannot move watermark time backwards from %s to %s",
        watermark.getMillis(), newWatermark.getMillis());
    logInteraction("Advancing watermark to %d", newWatermark.getMillis());
    watermark = newWatermark;
    timerManager.advanceWatermark(triggerExecutor, newWatermark);
  }

  /** Advance the processing time to the specified time, firing any timers that should fire. */
  public void advanceProcessingTime(
      Instant newProcessingTime) throws Exception {
    Preconditions.checkState(!newProcessingTime.isBefore(processingTime),
        "Cannot move processing time backwards from %s to %s",
        processingTime.getMillis(), newProcessingTime.getMillis());
    logInteraction("Advancing processing time to %d", newProcessingTime.getMillis());
    processingTime = newProcessingTime;
    timerManager.advanceProcessingTime(triggerExecutor, newProcessingTime);
  }

  public void injectElement(InputT value, Instant timestamp) throws Exception {
    Collection<W> windows = windowFn.assignWindows(new TriggerTester.StubAssignContext<W>(
        windowFn, value, timestamp, Arrays.asList(GlobalWindow.INSTANCE)));
    logInteraction("Element %s at time %d put in windows %s",
        value, timestamp.getMillis(), windows);
    triggerExecutor.onElement(WindowedValue.of(value, timestamp, windows));
  }

  public void doMerge() throws Exception {
    triggerExecutor.merge();
  }

  public void setTimer(
      W window, Instant timestamp, TimeDomain domain, ExecutableTrigger<W> trigger)
          throws CoderException {
    triggerExecutor.setTimer(
        new TriggerId<W>(window, trigger.getTriggerIndex()), timestamp, domain);
  }

  private class StubContexts
      implements WindowingInternals<InputT, KV<String, OutputT>>, DoFn.KeyedState {

    private Map<CodedTupleTag<?>, List<?>> tagListValues = new HashMap<>();
    private Map<CodedTupleTag<?>, Object> tagValues = new HashMap<>();
    private List<WindowedValue<KV<String, OutputT>>> outputs = new ArrayList<>();

    private Map<CodedTupleTag<?>, Instant> tagTimestamps = new HashMap<>();
    private PriorityQueue<Instant> minTagTimestamp = new PriorityQueue<>();

    @Override
    public void outputWindowedValue(KV<String, OutputT> output, Instant timestamp,
        Collection<? extends BoundedWindow> windows) {
      // Copy the output value (using coders) before capturing it.
      KV<String, OutputT> copy = SerializableUtils.<KV<String, OutputT>>ensureSerializableByCoder(
          KvCoder.of(StringUtf8Coder.of(), outputCoder), output, "outputForWindow");
      WindowedValue<KV<String, OutputT>> value = WindowedValue.of(copy, timestamp, windows);
      logInteraction("Outputting: %s", value);
      outputs.add(value);
    }

    public Set<String> getKeyedStateInUse() {
      return FluentIterable
          .from(tagListValues.keySet())
          .append(tagValues.keySet())
          .transform(new Function<CodedTupleTag<?>, String>() {
            @Override
            @Nullable
            public String apply(CodedTupleTag<?> input) {
              return input.getId();
            }
          })
          .toSet();
    }

    @Override
    public <T> void writeToTagList(CodedTupleTag<T> tag, T value) throws IOException {
      @SuppressWarnings("unchecked")
      List<T> values = (List<T>) tagListValues.get(tag);
      if (values == null) {
        values = new ArrayList<>();
        tagListValues.put(tag, values);
      }
      values.add(value);
    }

    @Override
    public <T> void deleteTagList(CodedTupleTag<T> tag) {
      tagListValues.remove(tag);
    }

    @Override
    public <T> Iterable<T> readTagList(CodedTupleTag<T> tag) {
      @SuppressWarnings("unchecked")
      List<T> values = (List<T>) tagListValues.get(tag);
      if (values == null) {
        return Collections.emptyList();
      } else {
        return values;
      }
    }

    @Override
    public <T> Map<CodedTupleTag<T>, Iterable<T>> readTagList(
        List<CodedTupleTag<T>> tags) throws IOException {
      return FluentIterable.from(tags)
          .toMap(new Function<CodedTupleTag<T>, Iterable<T>>() {
            @Override
            @Nullable
            public Iterable<T> apply(@Nullable CodedTupleTag<T> tag) {
              return readTagList(tag);
            }
          });
    }

    @Override
    public void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain) {
      throw new UnsupportedOperationException(
          "Testing triggers should not use timers from WindowingInternals.");
    }

    @Override
    public void deleteTimer(String timer, Trigger.TimeDomain domain) {
      throw new UnsupportedOperationException(
          "Testing triggers should not use timers from WindowingInternals.");
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      throw new UnsupportedOperationException(
          "Testing triggers should not use windows from WindowingInternals.");
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, T value) throws IOException {
      store(tag, value, BoundedWindow.TIMESTAMP_MAX_VALUE);
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, T value, Instant timestamp) throws IOException {
      tagValues.put(tag, value);

      // We never use the timestamp, but for testing purposes we want to keep track of the minimum
      // timestamp that is currently being stored, since this will be used to hold-up the watermark.
      Instant old = tagTimestamps.put(tag, timestamp);
      if (old != null) {
        minTagTimestamp.remove(old);
      }
      minTagTimestamp.add(timestamp);
    }

    @Override
    public <T> void remove(CodedTupleTag<T> tag) {
      tagValues.remove(tag);
      minTagTimestamp.remove(tagTimestamps.remove(tag));
    }

    @Override
    public <T> T lookup(CodedTupleTag<T> tag) throws IOException {
      @SuppressWarnings("unchecked")
      T value = (T) tagValues.get(tag);
      return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CodedTupleTagMap lookup(Iterable<? extends CodedTupleTag<?>> tags) throws IOException {
      LinkedHashMap<CodedTupleTag<?>, Object> result = new LinkedHashMap<>();
      for (CodedTupleTag<?> tag : tags) {
        result.put(tag, tagValues.get(tag));
      }
      return CodedTupleTagMap.of(result);
    }

    @Override
    public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data,
        Coder<T> elemCoder) throws IOException {
      throw new UnsupportedOperationException(
          "Testing triggers should not use writePCollectionViewData from WindowingInternals.");
    }
  }

  private class LoggingBatchTimerManager extends BatchTimerManager {

    public LoggingBatchTimerManager(Instant processingTime) {
      super(processingTime);
    }

    @Override
    public void setTimer(String tag, Instant timestamp, TimeDomain domain) {
      logInteraction("Setting timer '%s' for time %d in domain %s",
          tag, timestamp.getMillis(), domain);
      super.setTimer(tag, timestamp, domain);
    }

    @Override
    public void deleteTimer(String tag, Trigger.TimeDomain domain) {
      logInteraction("Delete timer '%s' in domain %s", tag, domain);
      super.deleteTimer(tag, domain);
    }

    @Override
    protected void fire(TriggerExecutor<?, ?, ?, ?> triggerExecutor,
        String tag, TimeDomain domain) throws Exception {
      logInteraction("Firing timer '%s' in domain %s", tag, domain);
      super.fire(triggerExecutor, tag, domain);
    }
  }

  private static class StubAssignContext<W extends BoundedWindow>
      extends WindowFn<Object, W>.AssignContext {
    private Object element;
    private Instant timestamp;
    private Collection<? extends BoundedWindow> windows;

    public StubAssignContext(WindowFn<Object, W> windowFn,
        Object element, Instant timestamp, Collection<? extends BoundedWindow> windows) {
      windowFn.super();
      this.element = element;
      this.timestamp = timestamp;
      this.windows = windows;
    }

    @Override
    public Object element() {
      return element;
    }

    @Override
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      return windows;
    }
  }

}
