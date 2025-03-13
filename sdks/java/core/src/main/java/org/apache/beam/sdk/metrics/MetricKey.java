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
import com.google.auto.value.extension.memoized.Memoized;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Metrics are keyed by the step name they are associated with and the name of the metric. */
@AutoValue
public abstract class MetricKey implements Serializable {

  /** The step name that is associated with this metric or Null if none is associated. */
  public abstract @Nullable String stepName();

  /** The name of the metric. */
  public abstract MetricName metricName();

  @Override
  @Memoized
  public String toString() {
    return stepName() + ":" + metricName();
  }

  public static MetricKey create(@Nullable String stepName, MetricName metricName) {
    return new AutoValue_MetricKey(stepName, metricName);
  }
}
