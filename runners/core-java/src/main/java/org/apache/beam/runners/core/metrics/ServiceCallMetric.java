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
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/*
 * Metric class which records Service API call metrics.
 * This class will capture a request count metric for the specified
 * request_count_urn and base_labels.
 * When Call() is invoked the status must be provided, which will
 * be converted to a canonical GCP status code, if possible.
 */
public class ServiceCallMetric {

  public static final Map<Integer, String> CANONICAL_STATUS_MAP =
      ImmutableMap.<Integer, String>builder()
          .put(200, "ok")
          .put(400, "out_of_range")
          .put(401, "unauthenticated")
          .put(403, "permission_denied")
          .put(404, "not_found")
          .put(409, "already_exists")
          .put(429, "resource_exhausted")
          .put(499, "cancelled")
          .put(500, "internal")
          .put(501, "not_implemented")
          .put(503, "unavailable")
          .put(504, "deadline_exceeded")
          .build();

  public static final String CANONICAL_STATUS_UNKNOWN = "unknown";

  public static final Map<String, String> STATUS_NORMALIZATION_MAP =
      ImmutableMap.<String, String>builder()
          .put("outofrange", "out_of_range")
          .put("permissiondenied", "permission_denied")
          .put("notfound", "not_found")
          .put("alreadyexists", "already_exists")
          .put("resourceexhausted", "resource_exhausted")
          .put("notimplemented", "not_implemented")
          .put("unavailable", "unavailable")
          .put("deadlineexceeded", "deadline_exceeded")
          .build();

  private HashMap<String, String> labels;
  private final String requestCountUrn;

  public ServiceCallMetric(String requestCountUrn, HashMap<String, String> baseLabels) {
    this.requestCountUrn = requestCountUrn;
    this.labels = baseLabels;
  }

  public void call(int httpStatusCode) {
    String canonicalStatusCode = ServiceCallMetric.convertToCanonicalStatusString(httpStatusCode);
    call(canonicalStatusCode);
  }

  public void call(String statusCode) {
    labels.put(
        MonitoringInfoConstants.Labels.STATUS,
        ServiceCallMetric.convertToCanonicalStatusString(statusCode));
    // MonitoringInfoMetricName will copy labels. So its safe to reuse this reference.
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(requestCountUrn, labels);
    Counter counter = LabeledMetrics.counter(name, true);
    counter.inc();
  }

  /** Converts an http status code to a canonical GCP status code string. */
  public static String convertToCanonicalStatusString(int httpStatusCode) {
    return CANONICAL_STATUS_MAP.getOrDefault(httpStatusCode, CANONICAL_STATUS_UNKNOWN);
  }

  /**
   * Converts an status code string to a canonical GCP status code string. This is used to make
   * strings like "notFound" to "not_found". If a mapping cannot be created known, then
   * statusCode.toLowerCase() will be returned.
   */
  public static String convertToCanonicalStatusString(String statusCode) {
    if (statusCode == null) {
      return CANONICAL_STATUS_UNKNOWN;
    }
    String normalizedStatus = STATUS_NORMALIZATION_MAP.get(statusCode.toLowerCase());
    if (normalizedStatus != null) {
      return normalizedStatus;
    }
    return statusCode.toLowerCase();
  }
}
