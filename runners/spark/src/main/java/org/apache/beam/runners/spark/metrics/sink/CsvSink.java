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
import java.lang.reflect.Constructor;
import java.util.Properties;
import org.apache.beam.runners.spark.metrics.AggregatorMetric;
import org.apache.beam.runners.spark.metrics.WithMetricsSupport;
import org.apache.spark.metrics.sink.Sink;

/**
 * A {@link Sink} for <a href="https://spark.apache.org/docs/latest/monitoring.html#metrics">Spark's
 * metric system</a> that is tailored to report {@link AggregatorMetric}s to a CSV file.
 *
 * <p>The sink is configured using Spark configuration parameters, for example:
 *
 * <pre>{@code
 * "spark.metrics.conf.*.sink.csv.class"="org.apache.beam.runners.spark.metrics.sink.CsvSink"
 * "spark.metrics.conf.*.sink.csv.directory"="<output_directory>"
 * "spark.metrics.conf.*.sink.csv.period"=10
 * "spark.metrics.conf.*.sink.csv.unit"=seconds
 * }</pre>
 */
public class CsvSink implements Sink {

  // Initialized reflectively as done by Spark's MetricsSystem
  private final org.apache.spark.metrics.sink.CsvSink delegate;

  /** Constructor for Spark 3.1.x. */
  public CsvSink(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final org.apache.spark.SecurityManager securityMgr) {
    delegate = newDelegate(properties, WithMetricsSupport.forRegistry(metricRegistry), securityMgr);
  }

  /** Constructor for Spark 3.2.x and later. */
  public CsvSink(final Properties properties, final MetricRegistry metricRegistry) {
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

  private static org.apache.spark.metrics.sink.CsvSink newDelegate(Object... params) {
    try {
      Constructor<?> constructor = org.apache.spark.metrics.sink.CsvSink.class.getConstructors()[0];
      return (org.apache.spark.metrics.sink.CsvSink) constructor.newInstance(params);
    } catch (ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }
}
