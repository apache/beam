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
package org.apache.beam.runners.core.metrics;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** Matchers for {@link MonitoringInfo}. */
public class MonitoringInfoMatchers {

  /**
   * Matches a {@link MonitoringInfo} with that has the set fields in the provide MonitoringInfo.
   *
   * <p>This is useful for tests which do not want to match the specific value (execution times).
   * Currently this will only check for URNs, labels, type URNs and int64Values.
   */
  public static TypeSafeMatcher<MonitoringInfo> matchSetFields(final MonitoringInfo mi) {
    return new TypeSafeMatcher<MonitoringInfo>() {

      @Override
      protected boolean matchesSafely(MonitoringInfo item) {
        if (!item.getUrn().equals(mi.getUrn())) {
          return false;
        }
        if (!item.getLabels().equals(mi.getLabels())) {
          return false;
        }
        if (!item.getType().equals(mi.getType())) {
          return false;
        }

        if (mi.getMetric().hasCounterData()) {
          long valueToMatch = mi.getMetric().getCounterData().getInt64Value();
          if (valueToMatch != item.getMetric().getCounterData().getInt64Value()) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("URN=")
            .appendValue(mi.getUrn())
            .appendText(", labels=")
            .appendValue(mi.getLabels())
            .appendText(", type=")
            .appendValue(mi.getType());
        if (mi.getMetric().hasCounterData()) {
          description
              .appendText(", value=")
              .appendValue(mi.getMetric().getCounterData().getInt64Value());
        }
      }
    };
  }

  /**
   * Matches a {@link MonitoringInfo} with that has the set fields in the provide MonitoringInfo.
   *
   * <p>This is useful for tests which do not want to match the specific value (execution times).
   * Currently this will only check for URNs, labels, type URNs and int64Values.
   */
  public static TypeSafeMatcher<MonitoringInfo> valueGreaterThan(final long value) {
    return new TypeSafeMatcher<MonitoringInfo>() {

      @Override
      protected boolean matchesSafely(MonitoringInfo item) {
        if (item.getMetric().getCounterData().getInt64Value() < value) {
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("value=").appendValue(value);
      }
    };
  }
}
