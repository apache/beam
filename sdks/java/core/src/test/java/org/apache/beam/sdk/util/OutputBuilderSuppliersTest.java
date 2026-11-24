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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link OutputBuilderSupplier} default methods and {@link OutputBuilderSuppliers}.
 */
@RunWith(JUnit4.class)
public class OutputBuilderSuppliersTest {

  private static final Instant TS = new Instant(1_700_000_000_000L);
  private static final Instant TS2 = TS.plus(Duration.standardSeconds(10));
  private static final IntervalWindow W1 =
      new IntervalWindow(TS, TS.plus(Duration.standardMinutes(1)));
  private static final IntervalWindow W2 =
      new IntervalWindow(
          TS.plus(Duration.standardMinutes(1)), TS.plus(Duration.standardMinutes(2)));
  private static final PaneInfo EARLY_PANE = PaneInfo.createPane(false, false, Timing.EARLY, 2, 1);

  private static Context activeSpan(String traceId) {
    return Context.root()
        .with(
            Span.wrap(
                SpanContext.create(
                    traceId,
                    "ff00000000000041",
                    TraceFlags.getSampled(),
                    TraceState.getDefault())));
  }

  @Test
  public void testDefaultInterfaceMethodsOnMinimalSupplier() {
    WindowedValue<String> template =
        WindowedValues.of(
            "in",
            TS,
            W1,
            EARLY_PANE,
            "rec-1",
            12L,
            CausedByDrain.CAUSED_BY_DRAIN,
            activeSpan("aa000000000000000000000000000001"),
            ValueKind.UPDATE_BEFORE);

    // Override ONLY builder(value) to exercise the default
    // build/buildWithTimestamp/buildWindowedValue.
    OutputBuilderSupplier minimal =
        new OutputBuilderSupplier() {
          @Override
          public <OutputT> WindowedValues.Builder<OutputT> builder(OutputT value) {
            return WindowedValues.builder(template, value);
          }
        };

    Context active = activeSpan("bb000000000000000000000000000002");
    try (Scope ignored = Context.root().makeCurrent()) {
      WindowedValue<Integer> built = minimal.build(42);
      assertEquals(Integer.valueOf(42), built.getValue());
      assertEquals(TS, built.getTimestamp());
      assertEquals(W1, WindowedValues.singleWindowOrNull(built));
      assertEquals(EARLY_PANE, built.getPaneInfo());
      assertEquals("rec-1", built.getRecordId());
      assertEquals(Long.valueOf(12L), built.getRecordOffset());
      assertEquals(CausedByDrain.CAUSED_BY_DRAIN, built.causedByDrain());
      assertEquals(ValueKind.UPDATE_BEFORE, built.getValueKind());
      assertNull(built.getOpenTelemetryContext());
    }

    try (Scope ignored = active.makeCurrent()) {
      WindowedValue<String> withTs = minimal.buildWithTimestamp("ts-out", TS2);
      assertEquals("ts-out", withTs.getValue());
      assertEquals(TS2, withTs.getTimestamp());
      assertSame(active, withTs.getOpenTelemetryContext());

      WindowedValue<String> win4 =
          minimal.buildWindowedValue(
              "win4", TS2, Collections.singletonList(W2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
      assertEquals("win4", win4.getValue());
      assertEquals(W2, WindowedValues.singleWindowOrNull(win4));
      assertEquals(PaneInfo.ON_TIME_AND_ONLY_FIRING, win4.getPaneInfo());
      assertEquals(ValueKind.UPDATE_BEFORE, win4.getValueKind());
      assertSame(active, win4.getOpenTelemetryContext());

      WindowedValue<String> win5 =
          minimal.buildWindowedValue(
              "win5", TS2, Arrays.asList(W1, W2), PaneInfo.NO_FIRING, ValueKind.DELETE);
      assertEquals(ValueKind.DELETE, win5.getValueKind());
      assertThat(win5.getWindows(), containsInAnyOrder(W1, W2));
      assertSame(active, win5.getOpenTelemetryContext());
    }
  }

  @Test
  public void testSupplierForElementAcrossWindowedValueShapes() {
    Context templateCtx = activeSpan("aa000000000000000000000000000001");
    Context activeCtx = activeSpan("cc000000000000000000000000000003");

    WindowedValue<?>[] templates = {
      WindowedValues.of(
          "gmin",
          BoundedWindow.TIMESTAMP_MIN_VALUE,
          GlobalWindow.INSTANCE,
          EARLY_PANE,
          "id-gmin",
          1L,
          CausedByDrain.CAUSED_BY_DRAIN,
          templateCtx,
          ValueKind.DELETE),
      WindowedValues.of(
          "gts",
          TS,
          GlobalWindow.INSTANCE,
          EARLY_PANE,
          "id-gts",
          2L,
          CausedByDrain.CAUSED_BY_DRAIN,
          templateCtx,
          ValueKind.UPDATE_AFTER),
      WindowedValues.of(
          "single",
          TS,
          W1,
          EARLY_PANE,
          "id-s",
          3L,
          CausedByDrain.CAUSED_BY_DRAIN,
          templateCtx,
          ValueKind.UPDATE_BEFORE),
      WindowedValues.of(
          "multi",
          TS,
          Arrays.asList(W1, W2),
          EARLY_PANE,
          "id-m",
          4L,
          CausedByDrain.CAUSED_BY_DRAIN,
          templateCtx,
          ValueKind.DELETE),
      WindowedValues.<String>builder()
          .setValue("b")
          .setTimestamp(TS)
          .setWindow(W1)
          .setPaneInfo(EARLY_PANE)
          .setRecordId("id-b")
          .setRecordOffset(5L)
          .setCausedByDrain(CausedByDrain.CAUSED_BY_DRAIN)
          .setValueKind(ValueKind.UPDATE_BEFORE)
    };

    for (WindowedValue<?> template : templates) {
      OutputBuilderSupplier supplier = OutputBuilderSuppliers.supplierForElement(template);
      try (Scope ignored = Context.root().makeCurrent()) {
        WindowedValue<String> built = supplier.build("out");
        assertEquals("out", built.getValue());
        assertEquals(template.getTimestamp(), built.getTimestamp());
        assertEquals(template.getPaneInfo(), built.getPaneInfo());
        assertEquals(template.getRecordId(), built.getRecordId());
        assertEquals(template.getRecordOffset(), built.getRecordOffset());
        assertEquals(template.causedByDrain(), built.causedByDrain());
        assertEquals(template.getValueKind(), built.getValueKind());
        assertEquals(template.getWindows().size(), built.getWindows().size());
        assertNull(built.getOpenTelemetryContext());

        WindowedValue<@Nullable String> builtNull = supplier.build(null);
        assertNull(builtNull.getValue());
      }

      try (Scope ignored = activeCtx.makeCurrent()) {
        WindowedValue<String> withTs = supplier.buildWithTimestamp("ts", TS2);
        assertEquals(TS2, withTs.getTimestamp());
        assertEquals(template.getRecordId(), withTs.getRecordId());
        assertSame(activeCtx, withTs.getOpenTelemetryContext());

        WindowedValue<String> fromBuilder = supplier.builder("via-builder").build();
        assertEquals("via-builder", fromBuilder.getValue());
        assertEquals(template.getRecordId(), fromBuilder.getRecordId());
        assertSame(activeCtx, fromBuilder.getOpenTelemetryContext());

        WindowedValue<String> explicit4 =
            supplier.buildWindowedValue(
                "exp4", TS2, Collections.singletonList(W2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
        assertEquals(W2, WindowedValues.singleWindowOrNull(explicit4));
        assertEquals(PaneInfo.ON_TIME_AND_ONLY_FIRING, explicit4.getPaneInfo());
        assertEquals(template.getValueKind(), explicit4.getValueKind());
        assertEquals(template.getRecordId(), explicit4.getRecordId());
        assertSame(activeCtx, explicit4.getOpenTelemetryContext());

        WindowedValue<String> explicit =
            supplier.buildWindowedValue(
                "exp", TS2, Collections.singletonList(W2), PaneInfo.NO_FIRING, ValueKind.INSERT);
        assertEquals(W2, WindowedValues.singleWindowOrNull(explicit));
        assertEquals(ValueKind.INSERT, explicit.getValueKind());
        assertEquals(template.getRecordId(), explicit.getRecordId());
        assertSame(activeCtx, explicit.getOpenTelemetryContext());
      }
    }
  }

  @Test
  public void testSupplierForElementRejectsNullTemplate() {
    assertThrows(
        NullPointerException.class, () -> OutputBuilderSuppliers.supplierForElement(castNull()));
  }

  @SuppressWarnings("nullness")
  private static WindowedValue<?> castNull() {
    return null;
  }
}
