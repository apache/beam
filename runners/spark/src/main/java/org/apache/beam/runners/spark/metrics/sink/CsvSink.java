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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Properties;
import org.apache.beam.runners.spark.metrics.AggregatorMetric;
import org.apache.beam.runners.spark.metrics.WithMetricsSupport;
import org.apache.spark.metrics.sink.Sink;

/**
 * A Spark {@link Sink} that is tailored to report {@link AggregatorMetric} metrics to a CSV file.
 */
// Intentionally overriding parent name because inheritors should replace the parent.
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_SUPERCLASS")
public class CsvSink extends org.apache.spark.metrics.sink.CsvSink {
  public CsvSink(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final org.apache.spark.SecurityManager securityMgr) {
    super(properties, WithMetricsSupport.forRegistry(metricRegistry), securityMgr);
  }
}
