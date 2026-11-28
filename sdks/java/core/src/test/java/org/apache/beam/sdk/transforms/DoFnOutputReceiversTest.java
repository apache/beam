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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.OutputBuilderSupplier;
import org.apache.beam.sdk.util.OutputBuilderSuppliers;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DoFnOutputReceivers}. */
@RunWith(JUnit4.class)
public class DoFnOutputReceiversTest {

  private static final Instant TS = new Instant(1_700_000_000_000L);
  private static final Instant TS2 = TS.plus(Duration.standardSeconds(5));
  private static final IntervalWindow W1 =
      new IntervalWindow(TS, TS.plus(Duration.standardMinutes(1)));
  private static final IntervalWindow W2 =
      new IntervalWindow(
          TS.plus(Duration.standardMinutes(1)), TS.plus(Duration.standardMinutes(2)));
  private static final PaneInfo PANE = PaneInfo.createPane(false, false, Timing.EARLY, 1, 0);
  private static final Schema ROW_SCHEMA = Schema.builder().addStringField("val").build();

  private static final class DummyDoFn<T> extends DoFn<String, T> {}

  private static final class RecordingHarness<T> {
    final List<WindowedValue<T>> mainOutputs = new ArrayList<>();
    final List<TaggedOutput> taggedOutputs = new ArrayList<>();
    final DoFn<String, T>.WindowedContext context;

    RecordingHarness() {
      PipelineOptions options = PipelineOptionsFactory.create();
      DoFn<String, T> doFn = new DummyDoFn<>();
      this.context =
          doFn.new WindowedContext() {
            @Override
            public PipelineOptions getPipelineOptions() {
              return options;
            }

            @Override
            public void output(T output) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void outputWithTimestamp(T output, Instant timestamp) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void outputWindowedValue(
                T output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo paneInfo) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void outputWindowedValue(WindowedValue<T> windowedValue) {
              mainOutputs.add(windowedValue);
            }

            @Override
            public <OutputT> void output(TupleTag<OutputT> tag, OutputT output) {
              throw new UnsupportedOperationException();
            }

            @Override
            public <OutputT> void outputWithTimestamp(
                TupleTag<OutputT> tag, OutputT output, Instant timestamp) {
              throw new UnsupportedOperationException();
            }

            @Override
            public <OutputT> void outputWindowedValue(
                TupleTag<OutputT> tag,
                OutputT output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo paneInfo) {
              throw new UnsupportedOperationException();
            }

            @Override
            public <OutputT> void outputWindowedValue(
                TupleTag<OutputT> tag, WindowedValue<OutputT> windowedValue) {
              taggedOutputs.add(new TaggedOutput(tag, windowedValue));
            }
          };
    }
  }

  private static final class TaggedOutput {
    final TupleTag<?> tag;
    final WindowedValue<?> value;

    TaggedOutput(TupleTag<?> tag, WindowedValue<?> value) {
      this.tag = tag;
      this.value = value;
    }
  }

  private static WindowedValue<String> metadataTemplate(Context templateCtx) {
    return WindowedValues.of(
        "template",
        TS,
        W1,
        PANE,
        "rid-42",
        99L,
        CausedByDrain.CAUSED_BY_DRAIN,
        templateCtx,
        ValueKind.UPDATE_BEFORE);
  }

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
  public void testWindowedReceiverUntaggedAndTaggedFastPaths() {
    Context templateCtx = activeSpan("aa000000000000000000000000000001");
    Context activeCtx = activeSpan("ff000000000000000000000000000041");
    OutputBuilderSupplier supplier =
        OutputBuilderSuppliers.supplierForElement(metadataTemplate(templateCtx));
    RecordingHarness<@Nullable String> harness = new RecordingHarness<>();

    OutputReceiver<@Nullable String> main =
        DoFnOutputReceivers.windowedReceiver(harness.context, supplier, null);
    TupleTag<@Nullable String> sideTag = new TupleTag<>("side");
    OutputReceiver<@Nullable String> side =
        DoFnOutputReceivers.windowedReceiver(harness.context, supplier, sideTag);

    try (Scope ignored = Context.root().makeCurrent()) {
      main.output("m1");
      main.output(null);
      side.output("s1");
    }
    assertEquals(2, harness.mainOutputs.size());
    assertMetadataPreserved(
        harness.mainOutputs.get(0), "m1", TS, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertNull(harness.mainOutputs.get(0).getOpenTelemetryContext());
    assertNull(harness.mainOutputs.get(1).getValue());
    assertEquals(1, harness.taggedOutputs.size());
    assertSame(sideTag, harness.taggedOutputs.get(0).tag);
    assertMetadataPreserved(
        harness.taggedOutputs.get(0).value, "s1", TS, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertNull(harness.taggedOutputs.get(0).value.getOpenTelemetryContext());

    try (Scope ignored = activeCtx.makeCurrent()) {
      main.outputWithTimestamp("m2", TS2);
      main.outputWindowedValue(
          "m3", TS2, Collections.singletonList(W2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
      main.outputWindowedValue(
          "m4", TS2, Arrays.asList(W1, W2), PaneInfo.NO_FIRING, ValueKind.DELETE);
      main.builder("m5").setRecordId("override-id").output();

      side.outputWithTimestamp("s2", TS2);
      side.outputWindowedValue(
          "s3", TS2, Collections.singletonList(W2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
      side.outputWindowedValue(
          "s4", TS2, Arrays.asList(W1, W2), PaneInfo.NO_FIRING, ValueKind.DELETE);
      side.builder("s5").setRecordOffset(123L).output();
    }

    assertEquals(6, harness.mainOutputs.size());
    assertMetadataPreserved(
        harness.mainOutputs.get(2), "m2", TS2, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertSame(activeCtx, harness.mainOutputs.get(2).getOpenTelemetryContext());
    assertMetadataPreserved(
        harness.mainOutputs.get(3),
        "m3",
        TS2,
        W2,
        PaneInfo.ON_TIME_AND_ONLY_FIRING,
        ValueKind.UPDATE_BEFORE);
    assertSame(activeCtx, harness.mainOutputs.get(3).getOpenTelemetryContext());
    assertEquals(ValueKind.DELETE, harness.mainOutputs.get(4).getValueKind());
    assertThat(harness.mainOutputs.get(4).getWindows(), containsInAnyOrder(W1, W2));
    assertEquals("override-id", harness.mainOutputs.get(5).getRecordId());
    assertSame(activeCtx, harness.mainOutputs.get(5).getOpenTelemetryContext());

    assertEquals(5, harness.taggedOutputs.size());
    assertMetadataPreserved(
        harness.taggedOutputs.get(1).value, "s2", TS2, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertMetadataPreserved(
        harness.taggedOutputs.get(2).value,
        "s3",
        TS2,
        W2,
        PaneInfo.ON_TIME_AND_ONLY_FIRING,
        ValueKind.UPDATE_BEFORE);
    assertEquals(ValueKind.DELETE, harness.taggedOutputs.get(3).value.getValueKind());
    assertEquals(Long.valueOf(123L), harness.taggedOutputs.get(4).value.getRecordOffset());
  }

  @Test
  public void testWindowedReceiverAcceptsWindowedValueAsPayloadWithoutRecursion() {
    OutputBuilderSupplier supplier =
        OutputBuilderSuppliers.supplierForElement(metadataTemplate(Context.root()));
    RecordingHarness<Object> harness = new RecordingHarness<>();
    OutputReceiver<Object> receiver =
        DoFnOutputReceivers.windowedReceiver(harness.context, supplier, null);

    WindowedValue<String> payload = WindowedValues.valueInGlobalWindow("inner");
    receiver.output(payload);
    assertEquals(1, harness.mainOutputs.size());
    assertSame(payload, harness.mainOutputs.get(0).getValue());
  }

  @Test
  public void testRowReceiverUntaggedAndTaggedAndMultiReceiver() {
    Context templateCtx = activeSpan("aa000000000000000000000000000001");
    Context activeCtx = activeSpan("dd000000000000000000000000000004");
    SchemaCoder<String> schemaCoder =
        SchemaCoder.of(
            ROW_SCHEMA,
            TypeDescriptor.of(String.class),
            s -> Row.withSchema(ROW_SCHEMA).addValue(s).build(),
            r -> "converted:" + r.getString(0));
    OutputBuilderSupplier supplier =
        OutputBuilderSuppliers.supplierForElement(metadataTemplate(templateCtx));
    RecordingHarness<String> harness = new RecordingHarness<>();
    TupleTag<String> sideTag = new TupleTag<>("rows");

    OutputReceiver<Row> mainRow =
        DoFnOutputReceivers.rowReceiver(harness.context, supplier, null, schemaCoder);
    Map<TupleTag<?>, Coder<?>> coders = Collections.singletonMap(sideTag, schemaCoder);
    MultiOutputReceiver multi =
        DoFnOutputReceivers.windowedMultiReceiver(harness.context, supplier, coders);
    OutputReceiver<Row> sideRow = multi.getRowReceiver(sideTag);
    OutputReceiver<String> sideTyped = multi.get(sideTag);

    try (Scope ignored = activeCtx.makeCurrent()) {
      mainRow.output(row("a"));
      mainRow.outputWithTimestamp(row("b"), TS2);
      mainRow.outputWindowedValue(
          row("c"), TS2, Collections.singletonList(W2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
      mainRow.outputWindowedValue(
          row("d"), TS2, Arrays.asList(W1, W2), PaneInfo.NO_FIRING, ValueKind.DELETE);
      mainRow.builder(row("e")).setRecordId("custom-row-id").output();

      sideRow.output(row("sa"));
      sideRow.outputWithTimestamp(row("sb"), TS2);
      sideRow.outputWindowedValue(
          row("sc"), TS2, Collections.singletonList(W2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
      sideRow.outputWindowedValue(
          row("sd"), TS2, Arrays.asList(W1, W2), PaneInfo.NO_FIRING, ValueKind.DELETE);
      sideRow.builder(row("se")).setRecordOffset(777L).output();

      sideTyped.output("typed-side");
    }

    assertEquals(5, harness.mainOutputs.size());
    assertMetadataPreserved(
        harness.mainOutputs.get(0), "converted:a", TS, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertSame(activeCtx, harness.mainOutputs.get(0).getOpenTelemetryContext());
    assertMetadataPreserved(
        harness.mainOutputs.get(1), "converted:b", TS2, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertMetadataPreserved(
        harness.mainOutputs.get(2),
        "converted:c",
        TS2,
        W2,
        PaneInfo.ON_TIME_AND_ONLY_FIRING,
        ValueKind.UPDATE_BEFORE);
    assertEquals("converted:d", harness.mainOutputs.get(3).getValue());
    assertEquals(ValueKind.DELETE, harness.mainOutputs.get(3).getValueKind());
    assertEquals("converted:e", harness.mainOutputs.get(4).getValue());
    assertEquals("custom-row-id", harness.mainOutputs.get(4).getRecordId());

    assertEquals(6, harness.taggedOutputs.size());
    assertMetadataPreserved(
        harness.taggedOutputs.get(0).value, "converted:sa", TS, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertMetadataPreserved(
        harness.taggedOutputs.get(1).value, "converted:sb", TS2, W1, PANE, ValueKind.UPDATE_BEFORE);
    assertMetadataPreserved(
        harness.taggedOutputs.get(2).value,
        "converted:sc",
        TS2,
        W2,
        PaneInfo.ON_TIME_AND_ONLY_FIRING,
        ValueKind.UPDATE_BEFORE);
    assertEquals("converted:sd", harness.taggedOutputs.get(3).value.getValue());
    assertEquals(ValueKind.DELETE, harness.taggedOutputs.get(3).value.getValueKind());
    assertEquals("converted:se", harness.taggedOutputs.get(4).value.getValue());
    assertEquals(Long.valueOf(777L), harness.taggedOutputs.get(4).value.getRecordOffset());
    assertEquals("typed-side", harness.taggedOutputs.get(5).value.getValue());
  }

  private static Row row(String s) {
    return Row.withSchema(ROW_SCHEMA).addValue(s).build();
  }

  private static void assertMetadataPreserved(
      WindowedValue<?> actual,
      Object expectedValue,
      Instant expectedTs,
      BoundedWindow expectedWindow,
      PaneInfo expectedPane,
      ValueKind expectedKind) {
    assertEquals(expectedValue, actual.getValue());
    assertEquals(expectedTs, actual.getTimestamp());
    assertEquals(expectedWindow, WindowedValues.singleWindowOrNull(actual));
    assertEquals(expectedPane, actual.getPaneInfo());
    assertEquals("rid-42", actual.getRecordId());
    assertEquals(Long.valueOf(99L), actual.getRecordOffset());
    assertEquals(CausedByDrain.CAUSED_BY_DRAIN, actual.causedByDrain());
    assertEquals(expectedKind, actual.getValueKind());
  }
}
