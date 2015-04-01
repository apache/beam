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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
 * @param <VI> The element types.
 * @param <VO> The final type for elements in the window (for instance, {@code Iterable<VI>})
 * @param <W> The type of windows being used.
 */
public class TriggerTester<VI, VO, W extends BoundedWindow> {

  private static final Logger LOGGER = Logger.getLogger(TriggerTester.class.getName());

  private Instant watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private BatchTimerManager timerManager = new LoggingBatchTimerManager(processingTime);

  private TriggerExecutor<String, VI, VO, W> triggerExecutor;

  private WindowFn<Object, W> windowFn;
  private StubContexts stubContexts;

  private static final String KEY = "TEST_KEY";

  private boolean logInteractions = false;

  private void logInteraction(String fmt, Object... args) {
    if (logInteractions) {
      LOGGER.warning("Trigger Interaction: " + String.format(fmt, args));
    }
  }

  public static <VI, VO, W extends BoundedWindow> TriggerTester<VI, VO, W> of(
      WindowFn<?, W> windowFn,
      Trigger<?, W> trigger,
      AbstractWindowSet.Factory<String, VI, VO, W> windowSetFactory) throws Exception {
    @SuppressWarnings("unchecked")
    WindowFn<Object, W> objectWindowFn = (WindowFn<Object, W>) windowFn;
    @SuppressWarnings("unchecked")
    Trigger<Object, W> objectTrigger = (Trigger<Object, W>) trigger;

    return new TriggerTester<VI, VO, W>(objectWindowFn, objectTrigger, windowSetFactory);
  }

  private TriggerTester(
      WindowFn<Object, W> windowFn,
      Trigger<Object, W> trigger,
      AbstractWindowSet.Factory<String, VI, VO, W> windowSetFactory) throws Exception {
    StubContexts stubContexts = new StubContexts();
    AbstractWindowSet<String, VI, VO, W> windowSet = windowSetFactory.create(
        KEY, windowFn.windowCoder(), stubContexts, stubContexts);

    this.windowFn = windowFn;
    this.stubContexts = stubContexts;
    this.triggerExecutor = new TriggerExecutor<>(
        windowFn, timerManager, trigger, stubContexts, stubContexts, windowSet);
  }

  public void logInteractions(boolean logInteractions) {
    this.logInteractions = logInteractions;
  }

  public void advanceWatermark(Instant newWatermark) throws Exception {
    Preconditions.checkState(!newWatermark.isBefore(watermark));
    logInteraction("Advancing watermark to %d", newWatermark.getMillis());
    watermark = newWatermark;
    timerManager.advanceWatermark(triggerExecutor, newWatermark);
  }

  public void advanceProcessingTime(Instant newProcessingTime) throws Exception {
    Preconditions.checkState(!newProcessingTime.isBefore(processingTime));
    logInteraction("Advancing processing time to %d", newProcessingTime.getMillis());
    processingTime = newProcessingTime;
    timerManager.advanceProcessingTime(triggerExecutor, newProcessingTime);
  }

  public void assertNoMoreOutput() {
    assertThat(stubContexts.outputs, Matchers.empty());
  }

  public void assertNextOutput(Instant outputTimestamp, Matcher<? super VO> element) {
    assertNextOutput(outputTimestamp, element, null);
  }

  public void assertNextOutput(Matcher<? super VO> element, BoundedWindow window) {
    assertNextOutput(window.maxTimestamp(), element, window);
  }

  private void assertNextOutput(Instant outputTimestamp, Matcher<? super VO> element,
      BoundedWindow window) {
    assertThat(stubContexts.outputs.size(), Matchers.greaterThan(0));
    WindowedValue<KV<String, VO>> first = stubContexts.outputs.remove(0);
    assertEquals(outputTimestamp, first.getTimestamp());
    assertEquals(first.getValue().getKey(), KEY);
    assertThat(first.getValue().getValue(), element);

    if (window != null) {
      assertThat(first.getWindows(), Matchers.contains(window));
    }
  }

  public void injectElement(VI value, Instant timestamp) throws Exception {
    Collection<W> windows = windowFn.assignWindows(new TriggerTester.StubAssignContext<W>(
        windowFn, value, timestamp, Arrays.asList(GlobalWindow.INSTANCE)));
    logInteraction("Element %s at time %d put in windows %s",
        value, timestamp.getMillis(), windows);
    triggerExecutor.onElement(WindowedValue.of(value, timestamp, windows));
  }

  private class StubContexts implements WindowingInternals<VI, KV<String, VO>>, DoFn.KeyedState {

    private Map<CodedTupleTag<?>, List<?>> tagListValues = new HashMap<>();
    private Map<CodedTupleTag<?>, Object> tagValues = new HashMap<>();
    private List<WindowedValue<KV<String, VO>>> outputs = new ArrayList<>();

    @Override
    public void outputWindowedValue(KV<String, VO> output, Instant timestamp,
        Collection<? extends BoundedWindow> windows) {
      WindowedValue<KV<String, VO>> value = WindowedValue.of(output, timestamp, windows);
      logInteraction("Outputting: %s", value);
      outputs.add(value);
    }

    @Override
    public <T> void writeToTagList(CodedTupleTag<T> tag, T value, Instant timestamp)
        throws IOException {
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
    public <T> Iterable<T> readTagList(CodedTupleTag<T> tag) throws IOException {
      @SuppressWarnings("unchecked")
      List<T> values = (List<T>) tagListValues.get(tag);
      return values == null ? Collections.<T>emptyList() : values;
    }

    @Override
    public void setTimer(String timer, Instant timestamp) {
      throw new UnsupportedOperationException(
          "Testing triggers should not use timers from WindowingInternals.");
    }

    @Override
    public void deleteTimer(String timer) {
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
      tagValues.put(tag, value);
    }

    @Override
    public <T> void remove(CodedTupleTag<T> tag) {
      tagValues.remove(tag);
    }

    @Override
    public <T> T lookup(CodedTupleTag<T> tag) throws IOException {
      @SuppressWarnings("unchecked")
      T value = (T) tagValues.get(tag);
      return value;
    }

    @Override
    public CodedTupleTagMap lookup(List<? extends CodedTupleTag<?>> tags) throws IOException {
      Set<CodedTupleTag<?>> tagSet = new LinkedHashSet<>(tags);
      return CodedTupleTagMap.of(Maps.asMap(tagSet, new Function<CodedTupleTag<?>, Object>() {
        @Override
        @Nullable
        public Object apply(@Nullable CodedTupleTag<?> tag) {
          return tagValues.get(tag);
        }
      }));
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
