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
import java.util.Properties;
import org.apache.beam.runners.spark.structuredstreaming.metrics.WithMetricsSupport;
import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.sink.Sink;

/**
 * A {@link Sink} for <a href="https://spark.apache.org/docs/latest/monitoring.html#metrics">Spark's
 * metric system</a> reporting metrics (including Beam step metrics) to a CSV file.
 *
 * <p>The sink is configured using Spark configuration parameters, for example:
 *
 * <pre>{@code
 * "spark.metrics.conf.*.sink.csv.class"="org.apache.beam.runners.spark.structuredstreaming.metrics.sink.CodahaleCsvSink"
 * "spark.metrics.conf.*.sink.csv.directory"="<output_directory>"
 * "spark.metrics.conf.*.sink.csv.period"=10
 * "spark.metrics.conf.*.sink.csv.unit"=seconds
 * }</pre>
 */
public class CodahaleCsvSink implements Sink {

  // Initialized reflectively as done by Spark's MetricsSystem
  private final org.apache.spark.metrics.sink.CsvSink delegate;

  /** Constructor for Spark 3.1.x and earlier. */
  public CodahaleCsvSink(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final SecurityManager securityMgr) {
    try {
      delegate =
          org.apache.spark.metrics.sink.CsvSink.class
              .getConstructor(Properties.class, MetricRegistry.class, SecurityManager.class)
              .newInstance(properties, WithMetricsSupport.forRegistry(metricRegistry), securityMgr);
    } catch (ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Constructor for Spark 3.2.x and later. */
  public CodahaleCsvSink(final Properties properties, final MetricRegistry metricRegistry) {
    try {
      delegate =
          org.apache.spark.metrics.sink.CsvSink.class
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
