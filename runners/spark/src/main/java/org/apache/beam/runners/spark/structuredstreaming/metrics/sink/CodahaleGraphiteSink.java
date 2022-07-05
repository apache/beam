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
package org.apache.beam.runners.spark.structuredstreaming.metrics.sink;

import com.codahale.metrics.MetricRegistry;
import java.lang.reflect.Constructor;
import java.util.Properties;
import org.apache.beam.runners.spark.structuredstreaming.metrics.AggregatorMetric;
import org.apache.beam.runners.spark.structuredstreaming.metrics.WithMetricsSupport;
import org.apache.spark.metrics.sink.Sink;

/**
 * A {@link Sink} for <a href="https://spark.apache.org/docs/latest/monitoring.html#metrics">Spark's
 * metric system</a> that is tailored to report {@link AggregatorMetric}s to Graphite.
 *
 * <p>The sink is configured using Spark configuration parameters, for example:
 *
 * <pre>{@code
 * "spark.metrics.conf.*.sink.graphite.class"="org.apache.beam.runners.spark.structuredstreaming.metrics.sink.CodahaleGraphiteSink"
 * "spark.metrics.conf.*.sink.graphite.host"="<graphite_hostname>"
 * "spark.metrics.conf.*.sink.graphite.port"=<graphite_listening_port>
 * "spark.metrics.conf.*.sink.graphite.period"=10
 * "spark.metrics.conf.*.sink.graphite.unit"=seconds
 * "spark.metrics.conf.*.sink.graphite.prefix"="<optional_prefix>"
 * "spark.metrics.conf.*.sink.graphite.regex"="<optional_regex_to_send_matching_metrics>"
 * }</pre>
 */
public class CodahaleGraphiteSink implements Sink {

  // Initialized reflectively as done by Spark's MetricsSystem
  private final org.apache.spark.metrics.sink.GraphiteSink delegate;

  /** Constructor for Spark 3.1.x. */
  public CodahaleGraphiteSink(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final org.apache.spark.SecurityManager securityMgr) {
    delegate = newDelegate(properties, WithMetricsSupport.forRegistry(metricRegistry), securityMgr);
  }

  /** Constructor for Spark 3.2.x and later. */
  public CodahaleGraphiteSink(final Properties properties, final MetricRegistry metricRegistry) {
    delegate = newDelegate(properties, WithMetricsSupport.forRegistry(metricRegistry));
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public void report() {
    delegate.report();
  }

  private static org.apache.spark.metrics.sink.GraphiteSink newDelegate(Object... params) {
    try {
      Constructor<?> constructor =
          org.apache.spark.metrics.sink.GraphiteSink.class.getConstructors()[0];
      return (org.apache.spark.metrics.sink.GraphiteSink) constructor.newInstance(params);
    } catch (ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }
}
