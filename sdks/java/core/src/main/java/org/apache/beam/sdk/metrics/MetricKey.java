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
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/** Metrics are keyed by the step name they are associated with and the name of the metric. */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricKey implements Serializable {

  /** The step name that is associated with this metric or Null if none is associated. */
  @Nullable
  public abstract String stepName();

  /** The name of the metric. */
  public abstract MetricName metricName();

  @Override
  public String toString() {
    return toString(":");
  }

  /**
   * Convenient representation of a metric's identifying information, with an arbitrary delimiter.
   *
   * <p>Metrics currently have either a {@link MetricKey#stepName ptransform-scope} or a
   * pcollection-scope; whichever label is present has its value inlined in this string
   * representation as the first delimited segment.
   *
   * <p>These two metric scopes are currently stored in different places, with only the
   * ptransform-scope handled here, but will be unified in the future.
   */
  public String toString(String delimiter) {
    if (stepName() == null) {
      return metricName().toString(delimiter);
    }
    return String.join(delimiter, stepName(), metricName().toString(delimiter));
  }

  public static MetricKey create(@Nullable String stepName, MetricName metricName) {
    return new AutoValue_MetricKey(stepName, metricName);
  }
}
