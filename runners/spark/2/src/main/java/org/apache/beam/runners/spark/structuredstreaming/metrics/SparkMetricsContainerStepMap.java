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
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Sole purpose of this class is to override {@link #toString()} of {@link MetricsContainerStepMap}
 * in order to show meaningful metrics in Spark Web Interface.
 */
class SparkMetricsContainerStepMap extends MetricsContainerStepMap {

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
