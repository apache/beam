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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Instant;

/** Implementations of {@link OutputBuilderSupplier}. */
@Internal
public class OutputBuilderSuppliers {
  private OutputBuilderSuppliers() {}

  /**
   * Returns an {@link OutputBuilderSupplier} that copies metadata from {@code templateValue}
   * without retaining its OpenTelemetry {@code Context}, avoiding intermediate {@link
   * WindowedValues.Builder} allocations by delegating directly to {@link
   * WindowedValues#ofWithCurrentContext(WindowedValue, Object)} and its sibling overloads.
   *
   * @param templateValue the non-null template {@link WindowedValue}
   * @return an {@link OutputBuilderSupplier} backed by {@code templateValue}
   */
  public static OutputBuilderSupplier supplierForElement(WindowedValue<?> templateValue) {
    return new ElementOutputBuilderSupplier(templateValue);
  }

  private static final class ElementOutputBuilderSupplier implements OutputBuilderSupplier {
    private final WindowedValue<?> templateValue;

    private ElementOutputBuilderSupplier(WindowedValue<?> templateValue) {
      this.templateValue = checkNotNull(templateValue, "templateValue must not be null");
    }

    @Override
    public <T> WindowedValues.Builder<T> builder(T value) {
      return WindowedValues.builder(templateValue, value);
    }

    @Override
    public <T> WindowedValue<T> build(T value) {
      return WindowedValues.ofWithCurrentContext(templateValue, value);
    }

    @Override
    public <T> WindowedValue<T> buildWithTimestamp(T value, Instant timestamp) {
      return WindowedValues.ofWithCurrentContext(templateValue, value, timestamp);
    }

    @Override
    public <T> WindowedValue<T> buildWindowedValue(
        T value,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      return WindowedValues.ofWithCurrentContext(
          templateValue, value, timestamp, windows, paneInfo);
    }

    @Override
    public <T> WindowedValue<T> buildWindowedValue(
        T value,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo,
        ValueKind valueKind) {
      return WindowedValues.ofWithCurrentContext(
          templateValue, value, timestamp, windows, paneInfo, valueKind);
    }
  }
}
