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

package org.apache.beam.sdk.metrics;

import java.util.Objects;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matchers for {@link MetricResults}.
 */
public class MetricResultsMatchers {

  /**
   * Matches a {@link MetricResult} with the given namespace, name and step, and whose value equals
   * the given value for attempted metrics.
   */
  public static <T> Matcher<MetricResult<T>> attemptedMetricsResult(
      final String namespace, final String name, final String step, final T value) {
    return metricsResult(namespace, name, step, value, false);
  }

  /**
   * Matches a {@link MetricResult} with the given namespace, name and step, and whose value equals
   * the given value for committed metrics.
   */
  public static <T> Matcher<MetricResult<T>> committedMetricsResult(
      final String namespace, final String name, final String step, final T value) {
    return metricsResult(namespace, name, step, value, true);
  }

  /**
   * Matches a {@link MetricResult} with the given namespace, name and step, and whose value equals
   * the given value for either committed or attempted (based on {@code isCommitted}) metrics.
   */
  public static <T> Matcher<MetricResult<T>> metricsResult(
      final String namespace, final String name, final String step, final T value,
      final boolean isCommitted) {
    final String metricState = isCommitted ? "committed" : "attempted";
    return new TypeSafeMatcher<MetricResult<T>>() {
      @Override
      protected boolean matchesSafely(MetricResult<T> item) {
        final T metricValue = isCommitted ? item.getCommitted() : item.getAttempted();
        return Objects.equals(namespace, item.getName().getNamespace())
            && Objects.equals(name, item.getName().getName())
            && item.getStep().contains(step)
            && metricResultsEqual(value, metricValue);
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(String.format(", %s=", metricState)).appendValue(value)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<T> item, Description mismatchDescription) {
        mismatchDescription.appendText("MetricResult{");
        final T metricValue = isCommitted ? item.getCommitted() : item.getAttempted();

        describeMetricsResultMembersMismatch(item, mismatchDescription, namespace, name, step);

        if (!Objects.equals(value, metricValue)) {
          mismatchDescription
              .appendText(String.format("%s: ", metricState)).appendValue(value)
              .appendText(" != ").appendValue(metricValue);
        }

        mismatchDescription.appendText("}");
      }
    };
  }

  private static <T> boolean metricResultsEqual(T result1, T result2) {
    if (result1 instanceof GaugeResult) {
      return (((GaugeResult) result1).getValue()) == (((GaugeResult) result2).getValue());
    } else {
      return Objects.equals(result1, result2);
    }
  }

  static Matcher<MetricResult<DistributionResult>> distributionAttemptedMinMax(
      final String namespace, final String name, final String step,
      final Long attemptedMin, final Long attemptedMax) {
    return distributionMinMax(namespace, name, step, attemptedMin, attemptedMax, false);
  }

  static Matcher<MetricResult<DistributionResult>> distributionCommittedMinMax(
      final String namespace, final String name, final String step,
      final Long committedMin, final Long committedMax) {
    return distributionMinMax(namespace, name, step, committedMin, committedMax, true);
  }

  public static Matcher<MetricResult<DistributionResult>> distributionMinMax(
      final String namespace, final String name, final String step,
      final Long min, final Long max, final boolean isCommitted) {
    final String metricState = isCommitted ? "committed" : "attempted";
    return new TypeSafeMatcher<MetricResult<DistributionResult>>() {
      @Override
      protected boolean matchesSafely(MetricResult<DistributionResult> item) {
        DistributionResult metricValue = isCommitted ? item.getCommitted() : item.getAttempted();
        return Objects.equals(namespace, item.getName().getNamespace())
            && Objects.equals(name, item.getName().getName())
            && item.getStep().contains(step)
            && Objects.equals(min, metricValue.getMin())
            && Objects.equals(max, metricValue.getMax());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(String.format(", %sMin=", metricState)).appendValue(min)
            .appendText(String.format(", %sMax=", metricState)).appendValue(max)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<DistributionResult> item,
          Description mismatchDescription) {
        mismatchDescription.appendText("MetricResult{");

        describeMetricsResultMembersMismatch(item, mismatchDescription, namespace, name, step);
        DistributionResult metricValue = isCommitted ? item.getCommitted() : item.getAttempted();

        if (!Objects.equals(min, metricValue.getMin())) {
          mismatchDescription
              .appendText(String.format("%sMin: ", metricState)).appendValue(min)
              .appendText(" != ").appendValue(metricValue.getMin());
        }

        if (!Objects.equals(max, metricValue.getMax())) {
          mismatchDescription
              .appendText(String.format("%sMax: ", metricState)).appendValue(max)
              .appendText(" != ").appendValue(metricValue.getMax());
        }

        mismatchDescription.appendText("}");
      }
    };
  }

  private static <T> void describeMetricsResultMembersMismatch(
      MetricResult<T> item,
      Description mismatchDescription,
      String namespace,
      String name,
      String step) {
    if (!Objects.equals(namespace, item.getName().getNamespace())) {
      mismatchDescription
          .appendText("inNamespace: ").appendValue(namespace)
          .appendText(" != ").appendValue(item.getName().getNamespace());
    }

    if (!Objects.equals(name, item.getName().getName())) {
      mismatchDescription
          .appendText("name: ").appendValue(name)
          .appendText(" != ").appendValue(item.getName().getName());
    }

    if (!item.getStep().contains(step)) {
      mismatchDescription
          .appendText("step: ").appendValue(step)
          .appendText(" != ").appendValue(item.getStep());
    }
  }
}
