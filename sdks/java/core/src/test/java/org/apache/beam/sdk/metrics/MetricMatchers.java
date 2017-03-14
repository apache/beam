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
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matchers for metrics.
 */
public class MetricMatchers {

  /**
   * Matches a {@link MetricUpdate} with the given name and contents.
   *
   * <p>Visible since it may be used in runner-specific tests.
   */
  public static <T> Matcher<MetricUpdate<T>> metricUpdate(final String name, final T update) {
    return new TypeSafeMatcher<MetricUpdate<T>>() {
      @Override
      protected boolean matchesSafely(MetricUpdate<T> item) {
        return Objects.equals(name, item.getKey().metricName().name())
            && Objects.equals(update, item.getUpdate());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricUpdate{name=").appendValue(name)
            .appendText(", update=").appendValue(update)
            .appendText("}");
      }
    };
  }

  /**
   * Matches a {@link MetricUpdate} with the given namespace, name, step and contents.
   *
   * <p>Visible since it may be used in runner-specific tests.
   */
  public static <T> Matcher<MetricUpdate<T>> metricUpdate(
      final String namespace, final String name, final String step, final T update) {
    return new TypeSafeMatcher<MetricUpdate<T>>() {
      @Override
      protected boolean matchesSafely(MetricUpdate<T> item) {
        return Objects.equals(namespace, item.getKey().metricName().namespace())
            && Objects.equals(name, item.getKey().metricName().name())
            && Objects.equals(step, item.getKey().stepName())
            && Objects.equals(update, item.getUpdate());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricUpdate{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(", update=").appendValue(update)
            .appendText("}");
      }
    };
  }

  /**
   * Matches a {@link MetricResult} with the given namespace, name and step, and whose attempted
   * value equals the given value.
   */
  public static <T> Matcher<MetricResult<T>> attemptedMetricsResult(
      final String namespace, final String name, final String step, final T attempted) {
    return new TypeSafeMatcher<MetricResult<T>>() {
      @Override
      protected boolean matchesSafely(MetricResult<T> item) {
        return Objects.equals(namespace, item.name().namespace())
            && Objects.equals(name, item.name().name())
            && item.step().contains(step)
            && metricResultsEqual(attempted, item.attempted());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(", attempted=").appendValue(attempted)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<T> item, Description mismatchDescription) {
        mismatchDescription.appendText("MetricResult{");

        describeMetricsResultMembersMismatch(item, mismatchDescription, namespace, name, step);

        if (!Objects.equals(attempted, item.attempted())) {
          mismatchDescription
              .appendText("attempted: ").appendValue(attempted)
              .appendText(" != ").appendValue(item.attempted());
        }

        mismatchDescription.appendText("}");
      }
    };
  }

  /**
   * Matches a {@link MetricResult} with the given namespace, name and step, and whose committed
   * value equals the given value.
   */
  public static <T> Matcher<MetricResult<T>> committedMetricsResult(
      final String namespace, final String name, final String step,
      final T committed) {
    return new TypeSafeMatcher<MetricResult<T>>() {
      @Override
      protected boolean matchesSafely(MetricResult<T> item) {
        return Objects.equals(namespace, item.name().namespace())
            && Objects.equals(name, item.name().name())
            && item.step().contains(step)
            && metricResultsEqual(committed, item.committed());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(", committed=").appendValue(committed)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<T> item, Description mismatchDescription) {
        mismatchDescription.appendText("MetricResult{");

        describeMetricsResultMembersMismatch(item, mismatchDescription, namespace, name, step);

        if (!Objects.equals(committed, item.committed())) {
          mismatchDescription
              .appendText("committed: ").appendValue(committed)
              .appendText(" != ").appendValue(item.committed());
        }

        mismatchDescription.appendText("}");
      }
    };
  }

  private static <T> boolean metricResultsEqual(T result1, T result2) {
    if (result1 instanceof GaugeResult) {
      return (((GaugeResult) result1).value()) == (((GaugeResult) result2).value());
    } else {
      return result1.equals(result2);
    }
  }

  static Matcher<MetricResult<DistributionResult>> distributionAttemptedMinMax(
      final String namespace, final String name, final String step,
      final Long attemptedMin, final Long attemptedMax) {
    return new TypeSafeMatcher<MetricResult<DistributionResult>>() {
      @Override
      protected boolean matchesSafely(MetricResult<DistributionResult> item) {
        return Objects.equals(namespace, item.name().namespace())
            && Objects.equals(name, item.name().name())
            && item.step().contains(step)
            && Objects.equals(attemptedMin, item.attempted().min())
            && Objects.equals(attemptedMax, item.attempted().max());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(", attemptedMin=").appendValue(attemptedMin)
            .appendText(", attemptedMax=").appendValue(attemptedMax)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<DistributionResult> item,
          Description mismatchDescription) {
        mismatchDescription.appendText("MetricResult{");

        describeMetricsResultMembersMismatch(item, mismatchDescription, namespace, name, step);

        if (!Objects.equals(attemptedMin, item.attempted())) {
          mismatchDescription
              .appendText("attemptedMin: ").appendValue(attemptedMin)
              .appendText(" != ").appendValue(item.attempted());
        }

        if (!Objects.equals(attemptedMax, item.attempted())) {
          mismatchDescription
              .appendText("attemptedMax: ").appendValue(attemptedMax)
              .appendText(" != ").appendValue(item.attempted());
        }

        mismatchDescription.appendText("}");
      }
    };
  }

  static Matcher<MetricResult<DistributionResult>> distributionCommittedMinMax(
      final String namespace, final String name, final String step,
      final Long committedMin, final Long committedMax) {
    return new TypeSafeMatcher<MetricResult<DistributionResult>>() {
      @Override
      protected boolean matchesSafely(MetricResult<DistributionResult> item) {
        return Objects.equals(namespace, item.name().namespace())
            && Objects.equals(name, item.name().name())
            && item.step().contains(step)
            && Objects.equals(committedMin, item.committed().min())
            && Objects.equals(committedMax, item.committed().max());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(", committedMin=").appendValue(committedMin)
            .appendText(", committedMax=").appendValue(committedMax)
            .appendText("}");
      }

      @Override
      protected void describeMismatchSafely(MetricResult<DistributionResult> item,
          Description mismatchDescription) {
        mismatchDescription.appendText("MetricResult{");

        describeMetricsResultMembersMismatch(item, mismatchDescription, namespace, name, step);

        if (!Objects.equals(committedMin, item.committed())) {
          mismatchDescription
              .appendText("committedMin: ").appendValue(committedMin)
              .appendText(" != ").appendValue(item.committed());
        }

        if (!Objects.equals(committedMax, item.committed())) {
          mismatchDescription
              .appendText("committedMax: ").appendValue(committedMax)
              .appendText(" != ").appendValue(item.committed());
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
    if (!Objects.equals(namespace, item.name().namespace())) {
      mismatchDescription
          .appendText("inNamespace: ").appendValue(namespace)
          .appendText(" != ").appendValue(item.name().namespace());
    }

    if (!Objects.equals(name, item.name().name())) {
      mismatchDescription
          .appendText("name: ").appendValue(name)
          .appendText(" != ").appendValue(item.name().name());
    }

    if (!item.step().contains(step)) {
      mismatchDescription
          .appendText("step: ").appendValue(step)
          .appendText(" != ").appendValue(item.step());
    }
  }
}
