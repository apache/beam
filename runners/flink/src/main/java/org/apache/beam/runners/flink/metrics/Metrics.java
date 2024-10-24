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
package org.apache.beam.runners.flink.metrics;

import java.util.Objects;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;

/** Helper for pretty-printing {@link Metric Flink metrics}. */
public class Metrics {
  public static String toString(Metric metric) {
    if (metric instanceof Counter) {
      return Long.toString(((Counter) metric).getCount());
    } else if (metric instanceof Gauge) {
      return Objects.toString(((Gauge<?>) metric).getValue());
    } else if (metric instanceof Meter) {
      return Double.toString(((Meter) metric).getRate());
    } else if (metric instanceof Histogram) {
      HistogramStatistics stats = ((Histogram) metric).getStatistics();
      return String.format(
          "count=%d, min=%d, max=%d, mean=%f, stddev=%f, p50=%f, p75=%f, p95=%f",
          stats.size(),
          stats.getMin(),
          stats.getMax(),
          stats.getMean(),
          stats.getStdDev(),
          stats.getQuantile(0.5),
          stats.getQuantile(0.75),
          stats.getQuantile(0.95));
    } else {
      throw new IllegalStateException(
          String.format(
              "Cannot remove unknown metric type %s. This indicates that the reporter "
                  + "does not support this metric type.",
              metric.getClass().getName()));
    }
  }
}
