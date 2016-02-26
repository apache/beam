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

package com.google.cloud.dataflow.sdk;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Instant;

import java.util.Collection;
import java.util.Objects;

/**
 * Matchers that are useful for working with Windowing, Timestamps, etc.
 */
public class WindowMatchers {

  public static <T> Matcher<WindowedValue<? extends T>> isWindowedValue(
      Matcher<? super T> valueMatcher, Matcher<? super Instant> timestampMatcher,
      Matcher<? super Collection<? extends BoundedWindow>> windowsMatcher) {
    return new WindowedValueMatcher<>(valueMatcher, timestampMatcher, windowsMatcher);
  }

  public static <T> Matcher<WindowedValue<? extends T>> isWindowedValue(
      Matcher<? super T> valueMatcher, Matcher<? super Instant> timestampMatcher) {
    return new WindowedValueMatcher<>(valueMatcher, timestampMatcher, Matchers.anything());
  }

  public static <T> Matcher<WindowedValue<? extends T>> isSingleWindowedValue(
      T value, long timestamp, long windowStart, long windowEnd) {
    return WindowMatchers.<T>isSingleWindowedValue(
        Matchers.equalTo(value), timestamp, windowStart, windowEnd);
  }

  public static <T> Matcher<WindowedValue<? extends T>> isSingleWindowedValue(
      Matcher<T> valueMatcher, long timestamp, long windowStart, long windowEnd) {
    IntervalWindow intervalWindow =
        new IntervalWindow(new Instant(windowStart), new Instant(windowEnd));
    return WindowMatchers.<T>isSingleWindowedValue(
        valueMatcher,
        Matchers.describedAs("%0", Matchers.equalTo(new Instant(timestamp)), timestamp),
        Matchers.<BoundedWindow>equalTo(intervalWindow));
  }

  public static <T> Matcher<WindowedValue<? extends T>> isSingleWindowedValue(
      Matcher<? super T> valueMatcher, Matcher<? super Instant> timestampMatcher,
      Matcher<? super BoundedWindow> windowMatcher) {
    return new WindowedValueMatcher<T>(
        valueMatcher, timestampMatcher, Matchers.contains(windowMatcher));
  }

  public static Matcher<IntervalWindow> intervalWindow(long start, long end) {
    return Matchers.equalTo(new IntervalWindow(new Instant(start), new Instant(end)));
  }

  public static <T> Matcher<WindowedValue<? extends T>> valueWithPaneInfo(final PaneInfo paneInfo) {
    return new TypeSafeMatcher<WindowedValue<? extends T>>() {
      @Override
      public void describeTo(Description description) {
        description
            .appendText("WindowedValue(paneInfo = ").appendValue(paneInfo).appendText(")");
      }

      @Override
      protected boolean matchesSafely(WindowedValue<? extends T> item) {
        return Objects.equals(item.getPane(), paneInfo);
      }

      @Override
      protected void describeMismatchSafely(
          WindowedValue<? extends T> item, Description mismatchDescription) {
        mismatchDescription.appendValue(item.getPane());
      }
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @SafeVarargs
  public static final <W extends BoundedWindow> Matcher<Iterable<W>> ofWindows(
      Matcher<W>... windows) {
    return (Matcher) Matchers.<W>containsInAnyOrder(windows);
  }

  private WindowMatchers() {}

  private static class WindowedValueMatcher<T> extends TypeSafeMatcher<WindowedValue<? extends T>> {

    private Matcher<? super T> valueMatcher;
    private Matcher<? super Instant> timestampMatcher;
    private Matcher<? super Collection<? extends BoundedWindow>> windowsMatcher;

    private WindowedValueMatcher(
        Matcher<? super T> valueMatcher,
        Matcher<? super Instant> timestampMatcher,
        Matcher<? super Collection<? extends BoundedWindow>> windowsMatcher) {
      this.valueMatcher = valueMatcher;
      this.timestampMatcher = timestampMatcher;
      this.windowsMatcher = windowsMatcher;
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a WindowedValue(").appendValue(valueMatcher)
          .appendText(", ").appendValue(timestampMatcher)
          .appendText(", ").appendValue(windowsMatcher)
          .appendText(")");
    }

    @Override
    protected boolean matchesSafely(WindowedValue<? extends T> windowedValue) {
      return valueMatcher.matches(windowedValue.getValue())
          && timestampMatcher.matches(windowedValue.getTimestamp())
          && windowsMatcher.matches(windowedValue.getWindows());
    }
  }
}
