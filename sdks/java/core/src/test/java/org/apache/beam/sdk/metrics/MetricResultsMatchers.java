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

/** Matchers for {@link MetricResults}. */
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
   * Matches a {@link MetricResult} with the given namespace, name and step, and a matcher for the
   * value for either committed or attempted (based on {@code isCommitted}) metrics.
   */
  public static <T> Matcher<MetricResult<T>> metricsResult(
      final String namespace,
      final String name,
      final String step,
      final Matcher<T> valueMatcher,
      final boolean isCommitted) {

    final String metricState = isCommitted ? "committed" : "attempted";
    return new MatchNameAndKey<T>(namespace, name, step) {
      @Override
      protected boolean matchesSafely(MetricResult<T> item) {
        final T metricValue = isCommitted ? item.getCommitted() : item.getAttempted();
        return super.matchesSafely(item) && valueMatcher.matches(metricValue);
      }

      @Override
      public void describeTo(Description description) {
        super.describeTo(description);
        description.appendText(String.format(", %s=", metricState));
        valueMatcher.describeTo(description);
        description.appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<T> item, Description mismatchDescription) {
        final T metricValue = isCommitted ? item.getCommitted() : item.getAttempted();
        super.describeMismatchSafely(item, mismatchDescription);
        mismatchDescription.appendText(String.format("%s: ", metricState));
        valueMatcher.describeMismatch(metricValue, mismatchDescription);
        mismatchDescription.appendText("}");
      }
    };
  }

  /**
   * Matches a {@link MetricResult} with the given namespace, name and step, and whose value equals
   * the given value for either committed or attempted (based on {@code isCommitted}) metrics.
   *
   * <p>For metrics with a {@link {@link GaugeResult}}, only the value is matched and the timestamp
   * is ignored.
   */
  public static <T> Matcher<MetricResult<T>> metricsResult(
      final String namespace,
      final String name,
      final String step,
      final T value,
      final boolean isCommitted) {
    final String metricState = isCommitted ? "committed" : "attempted";
    return new MatchNameAndKey<T>(namespace, name, step) {
      @Override
      protected boolean matchesSafely(MetricResult<T> item) {
        final T metricValue = isCommitted ? item.getCommitted() : item.getAttempted();
        return super.matchesSafely(item) && metricResultsEqual(value, metricValue);
      }

      @Override
      public void describeTo(Description description) {
        super.describeTo(description);
        description
            .appendText(String.format(", %s=", metricState))
            .appendValue(value)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<T> item, Description mismatchDescription) {
        final T metricValue = isCommitted ? item.getCommitted() : item.getAttempted();
        super.describeMismatchSafely(item, mismatchDescription);
        if (!Objects.equals(value, metricValue)) {
          mismatchDescription
              .appendText(String.format("%s: ", metricState))
              .appendValue(value)
              .appendText(" != ")
              .appendValue(metricValue);
        }

        mismatchDescription.appendText("}");
      }

      private boolean metricResultsEqual(T result1, T result2) {
        if (result1 instanceof GaugeResult) {
          return (((GaugeResult) result1).getValue()) == (((GaugeResult) result2).getValue());
        } else {
          return Objects.equals(result1, result2);
        }
      }
    };
  }

  static Matcher<MetricResult<DistributionResult>> distributionAttemptedMinMax(
      final String namespace,
      final String name,
      final String step,
      final Long attemptedMin,
      final Long attemptedMax) {
    return distributionMinMax(namespace, name, step, attemptedMin, attemptedMax, false);
  }

  static Matcher<MetricResult<DistributionResult>> distributionCommittedMinMax(
      final String namespace,
      final String name,
      final String step,
      final Long committedMin,
      final Long committedMax) {
    return distributionMinMax(namespace, name, step, committedMin, committedMax, true);
  }

  public static Matcher<MetricResult<DistributionResult>> distributionMinMax(
      final String namespace,
      final String name,
      final String step,
      final Long min,
      final Long max,
      final boolean isCommitted) {
    final String metricState = isCommitted ? "committed" : "attempted";
    return new MatchNameAndKey<DistributionResult>(namespace, name, step) {
      @Override
      protected boolean matchesSafely(MetricResult<DistributionResult> item) {
        final DistributionResult metricValue =
            isCommitted ? item.getCommitted() : item.getAttempted();
        return super.matchesSafely(item)
            && Objects.equals(min, metricValue.getMin())
            && Objects.equals(max, metricValue.getMax());
      }

      @Override
      public void describeTo(Description description) {
        super.describeTo(description);
        description
            .appendText(String.format(", %sMin=", metricState))
            .appendValue(min)
            .appendText(String.format(", %sMax=", metricState))
            .appendValue(max)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(
          MetricResult<DistributionResult> item, Description mismatchDescription) {
        final DistributionResult metricValue =
            isCommitted ? item.getCommitted() : item.getAttempted();
        super.describeMismatchSafely(item, mismatchDescription);
        if (!Objects.equals(min, metricValue.getMin())) {
          mismatchDescription
              .appendText(String.format("%sMin: ", metricState))
              .appendValue(min)
              .appendText(" != ")
              .appendValue(metricValue.getMin());
        }

        if (!Objects.equals(max, metricValue.getMax())) {
          mismatchDescription
              .appendText(String.format("%sMax: ", metricState))
              .appendValue(max)
              .appendText(" != ")
              .appendValue(metricValue.getMax());
        }

        mismatchDescription.appendText("}");
      }
    };
  }

  private static class MatchNameAndKey<T> extends TypeSafeMatcher<MetricResult<T>> {

    private final String namespace;
    private final String name;
    private final String step;

    MatchNameAndKey(String namespace, String name, String step) {
      this.namespace = namespace;
      this.name = name;
      this.step = step;
    }

    @Override
    protected boolean matchesSafely(MetricResult<T> item) {
      return MetricFiltering.matches(MetricsFilter.builder().addStep(step).build(), item.getKey())
          && Objects.equals(MetricName.named(namespace, name), item.getName());
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("MetricResult{inNamespace=")
          .appendValue(namespace)
          .appendText(", name=")
          .appendValue(name)
          .appendText(", step=")
          .appendValue(step);
      if (this.getClass() == MatchNameAndKey.class) {
        description.appendText("}");
      }
    }

    @Override
    protected void describeMismatchSafely(MetricResult<T> item, Description mismatchDescription) {
      mismatchDescription.appendText("MetricResult{");
      MetricKey key = MetricKey.create(step, MetricName.named(namespace, name));
      if (!Objects.equals(key, item.getKey())) {
        mismatchDescription
            .appendText("inKey: ")
            .appendValue(key)
            .appendText(" != ")
            .appendValue(item.getKey());
      }

      if (this.getClass() == MatchNameAndKey.class) {
        mismatchDescription.appendText("}");
      }
    }
  }
}
