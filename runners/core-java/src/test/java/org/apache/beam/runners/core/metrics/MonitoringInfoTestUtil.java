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

import java.util.HashMap;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;

/**
 * Provides convenient one line factories for unit tests that need to generate test MonitoringInfos.
 */
public class MonitoringInfoTestUtil {
  /** @return A basic MonitoringInfoMetricName to test. */
  public static MonitoringInfoMetricName testElementCountName() {
    HashMap labels = new HashMap<String, String>();
    labels.put(MonitoringInfoConstants.Labels.PCOLLECTION, "testPCollection");
    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
    return name;
  }

  /** @return A basic MonitoringInfo which matches the testElementCountName. */
  public static MonitoringInfo testElementCountMonitoringInfo(long value) {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "testPCollection");
    builder.setInt64SumValue(value);
    return builder.build();
  }
}
