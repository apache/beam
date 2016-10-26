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

  public static <T> Matcher<MetricResult<T>> metricResult(
      final String namespace, final String name, final String step,
      final T logical, final T physical) {
    return new TypeSafeMatcher<MetricResult<T>>() {
      @Override
      protected boolean matchesSafely(MetricResult<T> item) {
        return Objects.equals(namespace, item.name().namespace())
            && Objects.equals(name, item.name().name())
            && Objects.equals(step, item.step())
            && Objects.equals(logical, item.committed())
            && Objects.equals(physical, item.attempted());
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("MetricResult{inNamespace=").appendValue(namespace)
            .appendText(", name=").appendValue(name)
            .appendText(", step=").appendValue(step)
            .appendText(", logical=").appendValue(logical)
            .appendText(", physical=").appendValue(physical)
            .appendText("}");
      }
    };
  }

}
