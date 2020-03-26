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

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** Matchers for {@link MonitoringInfo}. */
public class MonitoringInfoMatchers {

  /**
   * Matches a {@link MonitoringInfo} with that has the set fields in the provided MonitoringInfo.
   *
   * <p>Currently this will only check for URNs, labels, type URNs and payloads.
   */
  public static TypeSafeMatcher<MonitoringInfo> matchSetFields(final MonitoringInfo mi) {
    return new TypeSafeMatcher<MonitoringInfo>() {

      @Override
      protected boolean matchesSafely(MonitoringInfo item) {
        return (mi.getUrn().isEmpty() || mi.getUrn().equals(item.getUrn()))
            && (mi.getLabelsMap().isEmpty() || mi.getLabelsMap().equals(item.getLabelsMap()))
            && (mi.getType().isEmpty() || mi.getType().equals(item.getType()))
            && (mi.getPayload().isEmpty() || mi.getPayload().equals(item.getPayload()));
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText("URN=")
            .appendValue(mi.getUrn())
            .appendText(", labels=")
            .appendValue(mi.getLabelsMap())
            .appendText(", type=")
            .appendValue(mi.getType())
            .appendText(", payload=")
            .appendValue(mi.getPayload());
      }
    };
  }

  /**
   * Matches a {@link MonitoringInfo} with a {@code value} greater then or equal to the {@code
   * value} supplied.
   *
   * <p>Currently this will only check for {@code beam:coder:varint:v1} encoded values.
   */
  public static TypeSafeMatcher<MonitoringInfo> counterValueGreaterThanOrEqualTo(final long value) {
    return new TypeSafeMatcher<MonitoringInfo>() {

      @Override
      protected boolean matchesSafely(MonitoringInfo item) {
        long decodedValue = decodeInt64Counter(item.getPayload());
        return decodedValue >= value;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("value=").appendValue(value);
      }
    };
  }
}
