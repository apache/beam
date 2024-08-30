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
package org.apache.beam.runners.spark.structuredstreaming.metrics;

import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AccumulatorV2} for Beam metrics captured in {@link MetricsContainerStepMap}.
 *
 * @see <a
 *     href="https://spark.apache.org/docs/latest/streaming-programming-guide.html#accumulators-broadcast-variables-and-checkpoints">accumulatorsV2</a>
 */
public class MetricsAccumulator
    extends AccumulatorV2<MetricsContainerStepMap, MetricsContainerStepMap> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsAccumulator.class);
  private static final MetricsContainerStepMap EMPTY = new SparkMetricsContainerStepMap();
  private static final String ACCUMULATOR_NAME = "Beam.Metrics";

  private static volatile @Nullable MetricsAccumulator instance = null;

  private MetricsContainerStepMap value;

  public MetricsAccumulator() {
    value = new SparkMetricsContainerStepMap();
  }

  private MetricsAccumulator(MetricsContainerStepMap value) {
    this.value = value;
  }

  @Override
  public boolean isZero() {
    return value.equals(EMPTY);
  }

  @Override
  public MetricsAccumulator copy() {
    MetricsContainerStepMap newContainer = new SparkMetricsContainerStepMap();
    newContainer.updateAll(value);
    return new MetricsAccumulator(newContainer);
  }

  @Override
  public void reset() {
    value = new SparkMetricsContainerStepMap();
  }

  @Override
  public void add(MetricsContainerStepMap other) {
    value.updateAll(other);
  }

  @Override
  public void merge(AccumulatorV2<MetricsContainerStepMap, MetricsContainerStepMap> other) {
    value.updateAll(other.value());
  }

  @Override
  public MetricsContainerStepMap value() {
    return value;
  }

  /**
   * Get the {@link MetricsAccumulator} on this driver. If there's no such accumulator yet, it will
   * be created and registered using the provided {@link SparkSession}.
   */
  public static MetricsAccumulator getInstance(SparkSession session) {
    MetricsAccumulator current = instance;
    if (current != null) {
      return current;
    }
    synchronized (MetricsAccumulator.class) {
      MetricsAccumulator accumulator = instance;
      if (accumulator == null) {
        accumulator = new MetricsAccumulator();
        session.sparkContext().register(accumulator, ACCUMULATOR_NAME);
        instance = accumulator;
        LOG.info("Instantiated metrics accumulator: {}", instance.value());
      }
      return accumulator;
    }
  }

  @VisibleForTesting
  public static void clear() {
    synchronized (MetricsAccumulator.class) {
      instance = null;
    }
  }

  /**
   * Sole purpose of this class is to override {@link #toString()} of {@link
   * MetricsContainerStepMap} in order to show meaningful metrics in Spark Web Interface.
   */
  private static class SparkMetricsContainerStepMap extends MetricsContainerStepMap {

    @Override
    public String toString() {
      return asAttemptedOnlyMetricResults(this).toString();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
