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

import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.common.collect.ImmutableList;

/**
 * The results of a query for metrics. Allows accessing all of the metrics that matched the filter.
 */
@AutoValue
@Experimental(Kind.METRICS)
public abstract class MetricQueryResults {
  /** Return the metric results for the getCounters that matched the filter. */
  public abstract Iterable<MetricResult<Long>> getCounters();

  /** Return the metric results for the getDistributions that matched the filter. */
  public abstract Iterable<MetricResult<DistributionResult>> getDistributions();

  /** Return the metric results for the getGauges that matched the filter. */
  public abstract Iterable<MetricResult<GaugeResult>> getGauges();

  static <T> void printMetrics(String type, Iterable<MetricResult<T>> metrics, PrintStream ps) {
    List<MetricResult<T>> metricsList = ImmutableList.copyOf(metrics);
    if (!metricsList.isEmpty()) {
      ps.print(type + "(");
      // ps.println(metricsList.size() + " " + type + ":");
      boolean first = true;
      for (MetricResult<T> metricResult : metricsList) {
        if (first) {
          first = false;
        } else {
          ps.print(", ");
        }
        MetricName name = metricResult.getName();
        ps.print(metricResult.getStep() + ":" + name.getNamespace() + ":" + name.getName());
        ps.print(": ");
        ps.print(metricResult.getAttempted());
        try {
          T committed = metricResult.getCommitted();
          ps.print(", ");
          ps.print(committed);
        } catch (UnsupportedOperationException ignored) {
        }
      }
      ps.print(")");
    }
  }

  @Override
  public String toString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    ps.print("MetricQueryResults(");
    printMetrics("Counters", getCounters(), ps);
    printMetrics("Distributions", getDistributions(), ps);
    printMetrics("Gauges", getGauges(), ps);
    ps.print(")");
    ps.close();
    return baos.toString();
  }

  public static MetricQueryResults create(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges) {
    return new AutoValue_MetricQueryResults(counters, distributions, gauges);
  }
}
