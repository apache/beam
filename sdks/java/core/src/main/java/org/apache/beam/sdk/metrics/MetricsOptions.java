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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** Extension of {@link PipelineOptions} that defines {@link MetricsSink} specific options. */
@Experimental(Kind.METRICS)
public interface MetricsOptions extends PipelineOptions {
  @Description("The beam sink class to which the metrics will be pushed")
  @Default.InstanceFactory(NoOpMetricsSink.class)
  Class<? extends MetricsSink> getMetricsSink();

  void setMetricsSink(Class<? extends MetricsSink> metricsSink);

  /**
   * A {@link DefaultValueFactory} that obtains the class of the {@code NoOpMetricsSink} if it
   * exists on the classpath, and throws an exception otherwise.
   *
   * <p>As the {@code NoOpMetricsSink} is in an independent module, it cannot be directly referenced
   * as the {@link Default}. However, it should still be used if available.
   */
  class NoOpMetricsSink implements DefaultValueFactory<Class<? extends MetricsSink>> {
    @Override
    public Class<? extends MetricsSink> create(PipelineOptions options) {
      try {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends MetricsSink> noOpMetricsSinkClass =
            (Class<? extends MetricsSink>)
                Class.forName(
                    "org.apache.beam.runners.core.metrics.NoOpMetricsSink",
                    true,
                    ReflectHelpers.findClassLoader());
        return noOpMetricsSinkClass;
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            String.format("NoOpMetricsSink was not found on classpath"));
      }
    }
  }

  @Description("The metrics push period in seconds")
  @Default.Long(5)
  Long getMetricsPushPeriod();

  void setMetricsPushPeriod(Long period);

  @Description("MetricsHttpSink url")
  String getMetricsHttpSinkUrl();

  void setMetricsHttpSinkUrl(String metricsSink);

  @Description("The graphite metrics host")
  String getMetricsGraphiteHost();

  void setMetricsGraphiteHost(String host);

  @Description("The graphite metrics port")
  @Default.Integer(2003)
  Integer getMetricsGraphitePort();

  void setMetricsGraphitePort(Integer port);
}
