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
package org.apache.beam.sdk.util;

import java.util.Collection;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Instant;

/**
 * Supplies {@link WindowedValues.Builder} instances and derived {@link WindowedValue}s populated
 * with template metadata for an emitted element.
 */
@Internal
@FunctionalInterface
public interface OutputBuilderSupplier {
  /**
   * Returns a fresh, independently mutable {@link WindowedValues.Builder} pre-populated with {@code
   * value} and the supplier's template metadata (so downstream callers may mutate it in place,
   * e.g., via {@link WindowedValues.Builder#setReceiver}).
   *
   * <p>The parameter {@code value} is required to fix the {@code OutputT} type variable.
   *
   * @param value the element value to initialize the returned builder with
   * @return a fresh {@link WindowedValues.Builder} containing {@code value}
   */
  <OutputT> WindowedValues.Builder<OutputT> builder(OutputT value);

  /**
   * Builds a {@link WindowedValue} for {@code value} using the supplier's template metadata and
   * capturing the current thread's OpenTelemetry {@code Context} (with {@code Context.root()}
   * normalized to {@code null}).
   *
   * <p>Note: overrides should match the behavior of {@code builder(value).build()} for valid
   * inputs, though exception types thrown on invalid {@code null} metadata may differ from the
   * default implementation.
   *
   * @param value the element value
   * @return a {@link WindowedValue} populated with {@code value} and template metadata
   */
  default <OutputT> WindowedValue<OutputT> build(OutputT value) {
    return builder(value).build();
  }

  /**
   * Builds a {@link WindowedValue} for {@code value} with the given non-null {@code timestamp}
   * using the supplier's template metadata and capturing the current thread's OpenTelemetry {@code
   * Context} (with {@code Context.root()} normalized to {@code null}).
   *
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @return a {@link WindowedValue} populated with {@code value}, {@code timestamp}, and template
   *     metadata
   */
  default <OutputT> WindowedValue<OutputT> buildWithTimestamp(OutputT value, Instant timestamp) {
    return builder(value).setTimestamp(timestamp).build();
  }

  /**
   * Builds a {@link WindowedValue} for {@code value} with explicit {@code timestamp}, {@code
   * windows}, and {@code paneInfo}, retaining the supplier's remaining template metadata (e.g.,
   * record id, record offset, drain status, {@link ValueKind}) and capturing the current thread's
   * OpenTelemetry {@code Context} (with {@code Context.root()} normalized to {@code null}).
   *
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @param windows the non-empty collection of windows
   * @param paneInfo the non-null {@link PaneInfo}
   * @return a {@link WindowedValue} with the specified windowing attributes and template metadata
   */
  default <OutputT> WindowedValue<OutputT> buildWindowedValue(
      OutputT value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo) {
    return builder(value).setTimestamp(timestamp).setWindows(windows).setPaneInfo(paneInfo).build();
  }

  /**
   * Builds a {@link WindowedValue} for {@code value} with explicit {@code timestamp}, {@code
   * windows}, {@code paneInfo}, and {@code valueKind}, retaining the supplier's remaining template
   * metadata (e.g., record id, record offset, drain status) and capturing the current thread's
   * OpenTelemetry {@code Context} (with {@code Context.root()} normalized to {@code null}).
   *
   * @param value the element value
   * @param timestamp the non-null event timestamp
   * @param windows the non-empty collection of windows
   * @param paneInfo the non-null {@link PaneInfo}
   * @param valueKind the non-null {@link ValueKind}
   * @return a {@link WindowedValue} with the specified attributes and template metadata
   */
  default <OutputT> WindowedValue<OutputT> buildWindowedValue(
      OutputT value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo,
      ValueKind valueKind) {
    return builder(value)
        .setTimestamp(timestamp)
        .setWindows(windows)
        .setPaneInfo(paneInfo)
        .setValueKind(valueKind)
        .build();
  }
}
