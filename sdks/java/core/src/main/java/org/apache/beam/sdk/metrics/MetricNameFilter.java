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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The name of a metric. */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricNameFilter {

  /** The inNamespace that a metric must be in to match this {@link MetricNameFilter}. */
  public abstract String getNamespace();

  /** If set, the metric must have this name to match this {@link MetricNameFilter}. */
  public abstract @Nullable String getName();

  public static MetricNameFilter inNamespace(String namespace) {
    return new AutoValue_MetricNameFilter(namespace, null);
  }

  public static MetricNameFilter inNamespace(Class<?> namespace) {
    return new AutoValue_MetricNameFilter(namespace.getName(), null);
  }

  public static MetricNameFilter named(String namespace, String name) {
    checkNotNull(name, "Must specify a name");
    return new AutoValue_MetricNameFilter(namespace, name);
  }

  public static MetricNameFilter named(Class<?> namespace, String name) {
    checkNotNull(namespace, "Must specify a inNamespace");
    checkNotNull(name, "Must specify a name");
    return new AutoValue_MetricNameFilter(namespace.getName(), name);
  }
}
