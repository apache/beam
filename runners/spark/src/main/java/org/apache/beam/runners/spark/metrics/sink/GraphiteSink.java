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
package org.apache.beam.runners.spark.metrics.sink;

import com.codahale.metrics.MetricRegistry;
import java.util.Properties;
import org.apache.beam.runners.spark.metrics.WithMetricsSupport;
import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.sink.Sink;

/**
 * A {@link Sink} for <a href="https://spark.apache.org/docs/latest/monitoring.html#metrics">Spark's
 * metric system</a> reporting metrics (including Beam step metrics) to Graphite.
 *
 * <p>The sink is configured using Spark configuration parameters, for example:
 *
 * <pre>{@code
 * "spark.metrics.conf.*.sink.graphite.class"="org.apache.beam.runners.spark.metrics.sink.GraphiteSink"
 * "spark.metrics.conf.*.sink.graphite.host"="<graphite_hostname>"
 * "spark.metrics.conf.*.sink.graphite.port"=<graphite_listening_port>
 * "spark.metrics.conf.*.sink.graphite.period"=10
 * "spark.metrics.conf.*.sink.graphite.unit"=seconds
 * "spark.metrics.conf.*.sink.graphite.prefix"="<optional_prefix>"
 * "spark.metrics.conf.*.sink.graphite.regex"="<optional_regex_to_send_matching_metrics>"
 * }</pre>
 */
public class GraphiteSink implements Sink {

  // Initialized reflectively as done by Spark's MetricsSystem
  private final org.apache.spark.metrics.sink.GraphiteSink delegate;

  /** Constructor for Spark 3.1.x and earlier. */
  public GraphiteSink(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final SecurityManager securityMgr) {
    try {
      delegate =
          org.apache.spark.metrics.sink.GraphiteSink.class
              .getConstructor(Properties.class, MetricRegistry.class, SecurityManager.class)
              .newInstance(properties, WithMetricsSupport.forRegistry(metricRegistry), securityMgr);
    } catch (ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Constructor for Spark 3.2.x and later. */
  public GraphiteSink(final Properties properties, final MetricRegistry metricRegistry) {
    try {
      delegate =
          org.apache.spark.metrics.sink.GraphiteSink.class
              .getConstructor(Properties.class, MetricRegistry.class)
              .newInstance(properties, WithMetricsSupport.forRegistry(metricRegistry));
    } catch (ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
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
}
