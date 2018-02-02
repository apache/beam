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

import com.google.common.collect.Iterables;
import java.util.Locale;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;

/** Serialize metrics into json representation to be pushed to a backend. */
public class JsonMetricsSerializer implements MetricsSerializer<String> {

  @Override public String serializeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    StringBuffer json = new StringBuffer();
    json.append("{");
    json.append("\"counters\":[");

    int i = 0;
    for (MetricResult<Long> result : metricQueryResults.counters()) {
      i++;
      json.append("{");
      String name = result.name().namespace() + "/" + result.name().name();
      json.append(String.format("\"name\":\"%s\",", name));
      String step = result.step();
      json.append(String.format("\"step\":\"%s\",", step));
      Long attempted = result.attempted();
      json.append(String.format("\"attempted\":%d", attempted));
      json.append("}");
      if (i < Iterables.size(metricQueryResults.counters())) {
        json.append(",");
      }
    }
    json.append("]");
    json.append(",");
    json.append("\"distributions\":[");
    i = 0;
    for (MetricResult<DistributionResult> result : metricQueryResults.distributions()) {
      i++;
      json.append("{");
      String name = result.name().namespace() + "/" + result.name().name();
      json.append(String.format("\"name\":\"%s\",", name));
      String step = result.step();
      json.append(String.format("\"step\":\"%s\",", step));
      DistributionResult attempted = result.attempted();
      json.append("\"attempted\":");
      json.append("{");

      json.append(String.format("\"min\":%d,", attempted.min()));
      json.append(String.format("\"max\":%d,", attempted.max()));
      json.append(String.format("\"sum\":%d,", attempted.sum()));
      json.append(String.format("\"count\":%d,", attempted.count()));
      json.append(String.format(Locale.ROOT, "\"mean\":%.3f", attempted.mean()));
      json.append("}");

      json.append("}");
      if (i < Iterables.size(metricQueryResults.distributions())) {
        json.append(",");
      }
    }
    json.append("]");
    json.append(",");
    json.append("\"gauges\":[");
    i = 0;
    for (MetricResult<GaugeResult> result : metricQueryResults.gauges()) {
      i++;
      json.append("{");
      String name = result.name().namespace() + "/" + result.name().name();
      json.append(String.format("\"name\":\"%s\",", name));
      String step = result.step();
      json.append(String.format("\"step\":\"%s\",", step));
      GaugeResult attempted = result.attempted();
      json.append("\"attempted\":");
      json.append("{");

      json.append(String.format("\"value\":%d,", attempted.value()));
      json.append(String.format("\"timestamp\":\"%s\"", attempted.timestamp().toString()));
      json.append("}");

      json.append("}");
      if (i < Iterables.size(metricQueryResults.gauges())) {
        json.append(",");
      }
    }
    json.append("]");

    json.append("}");
    return json.toString();
  }
}
