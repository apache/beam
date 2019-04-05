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

import org.apache.beam.sdk.metrics.MetricName;

/** Utility for parsing a URN to a {@link MetricName}. */
public class MetricUrns {
  /**
   * Parse a {@link MetricName} from a {@link
   * org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs.Enum}.
   *
   * <p>Should be consistent with {@code parse_namespace_and_name} in monitoring_infos.py.
   */
  public static MetricName parseUrn(String urn) {
    if (urn.startsWith(MonitoringInfoConstants.Urns.USER_COUNTER_PREFIX)) {
      urn = urn.substring(MonitoringInfoConstants.Urns.USER_COUNTER_PREFIX.length());
    }
    // If it is not a user counter, just use the first part of the URN, i.e. 'beam'
    String[] pieces = urn.split(":", 2);
    if (pieces.length != 2) {
      throw new IllegalArgumentException("Invalid metric URN: " + urn);
    }
    return MetricName.named(pieces[0], pieces[1]);
  }
}
