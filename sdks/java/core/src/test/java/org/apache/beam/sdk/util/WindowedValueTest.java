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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link WindowedValue}. */
@RunWith(JUnit4.class)
public class WindowedValueTest {

  @After
  public void tearDown() {
    WindowedValues.WindowedValueCoder.setMetadataNotSupported();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWindowedValueCoder() throws CoderException {
    Instant timestamp = new Instant(1234);
    WindowedValue<String> value =
        WindowedValues.of(
            "abc",
            new Instant(1234),
            Arrays.asList(
                new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1000))),
                new IntervalWindow(
                    timestamp.plus(Duration.millis(1000)), timestamp.plus(Duration.millis(2000)))),
            PaneInfo.NO_FIRING);

    Coder<WindowedValue<String>> windowedValueCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    byte[] encodedValue = CoderUtils.encodeToByteArray(windowedValueCoder, value);
    WindowedValue<String> decodedValue =
        CoderUtils.decodeFromByteArray(windowedValueCoder, encodedValue);

    Assert.assertEquals(value.getValue(), decodedValue.getValue());
    Assert.assertEquals(value.getTimestamp(), decodedValue.getTimestamp());
    Assert.assertArrayEquals(value.getWindows().toArray(), decodedValue.getWindows().toArray());
  }

  @Test
  public void testWindowedValueWithElementMetadataCoder() throws CoderException {
    WindowedValues.WindowedValueCoder.setMetadataSupported();

    Context context =
        Context.current()
            .with(
                Span.wrap(
                    SpanContext.create(
                        "ff000000000000000000000000000041",
                        "ff00000000000041",
                        TraceFlags.getSampled(),
                        TraceState.builder().put("foo", "bar").put("bar", "baz").build())));

    Instant timestamp = new Instant(1234);
    WindowedValue<String> value =
        WindowedValues.of(
            "abc",
            new Instant(1234),
            Arrays.asList(
                new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1000))),
                new IntervalWindow(
                    timestamp.plus(Duration.millis(1000)), timestamp.plus(Duration.millis(2000)))),
            PaneInfo.NO_FIRING,
            null,
            null,
            CausedByDrain.CAUSED_BY_DRAIN,
            context,
            ValueKind.DELETE); // drain is persisted as part of metadata

    Coder<WindowedValue<String>> windowedValueCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    byte[] encodedValue = CoderUtils.encodeToByteArray(windowedValueCoder, value);

    WindowedValue<String> decodedValue =
        CoderUtils.decodeFromByteArray(windowedValueCoder, encodedValue);

    Assert.assertEquals(value.getValue(), decodedValue.getValue());
    Assert.assertEquals(value.getTimestamp(), decodedValue.getTimestamp());
    Assert.assertArrayEquals(value.getWindows().toArray(), decodedValue.getWindows().toArray());
    Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, decodedValue.causedByDrain());
    Assert.assertNotNull(decodedValue.getOpenTelemetryContext());
    Assert.assertEquals(ValueKind.DELETE, decodedValue.getValueKind());
  }

  @Test
  public void testWindowedValueWithValueKindCoder() throws CoderException {
    WindowedValues.WindowedValueCoder.setMetadataSupported();
    Instant timestamp = new Instant(1234);
    WindowedValue<String> value =
        WindowedValues.<String>builder()
            .setValue("abc")
            .setTimestamp(timestamp)
            .setWindows(
                Arrays.asList(new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1000)))))
            .setPaneInfo(PaneInfo.NO_FIRING)
            .setValueKind(ValueKind.UPDATE_BEFORE)
            .build();

    Coder<WindowedValue<String>> windowedValueCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    byte[] encodedValue = CoderUtils.encodeToByteArray(windowedValueCoder, value);
    WindowedValue<String> decodedValue =
        CoderUtils.decodeFromByteArray(windowedValueCoder, encodedValue);

    Assert.assertEquals(value.getValue(), decodedValue.getValue());
    Assert.assertEquals(value.getTimestamp(), decodedValue.getTimestamp());
    Assert.assertArrayEquals(value.getWindows().toArray(), decodedValue.getWindows().toArray());
    Assert.assertEquals(value.getValueKind(), decodedValue.getValueKind());
  }

  @Test
  public void testFullWindowedValueCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        WindowedValues.getFullCoder(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testParamWindowedValueCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        WindowedValues.getParamWindowedValueCoder(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testValueOnlyWindowedValueCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        WindowedValues.getValueOnlyCoder(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testExplodeWindowsInNoWindowsCrash() {
    thrown.expect(IllegalArgumentException.class);
    WindowedValues.of("foo", Instant.now(), ImmutableList.of(), PaneInfo.NO_FIRING);
  }

  @Test
  public void testExplodeWindowsInOneWindowEquals() {
    Instant now = Instant.now();
    BoundedWindow window =
        new IntervalWindow(now.minus(Duration.millis(1000L)), now.plus(Duration.millis(1000L)));
    WindowedValue<String> value =
        WindowedValues.of("foo", now, window, PaneInfo.ON_TIME_AND_ONLY_FIRING);

    assertThat(Iterables.getOnlyElement(value.explodeWindows()), equalTo(value));
  }

  @Test
  public void testExplodeWindowsManyWindowsMultipleWindowedValues() {
    Instant now = Instant.now();
    BoundedWindow centerWindow =
        new IntervalWindow(now.minus(Duration.millis(1000L)), now.plus(Duration.millis(1000L)));
    BoundedWindow pastWindow =
        new IntervalWindow(now.minus(Duration.millis(1500L)), now.plus(Duration.millis(500L)));
    BoundedWindow futureWindow =
        new IntervalWindow(now.minus(Duration.millis(500L)), now.plus(Duration.millis(1500L)));
    BoundedWindow futureFutureWindow = new IntervalWindow(now, now.plus(Duration.millis(2000L)));
    PaneInfo paneInfo = PaneInfo.createPane(false, false, Timing.ON_TIME, 3L, 0L);
    WindowedValue<String> value =
        WindowedValues.of(
            "foo",
            now,
            ImmutableList.of(pastWindow, centerWindow, futureWindow, futureFutureWindow),
            paneInfo);

    assertThat(
        value.explodeWindows(),
        containsInAnyOrder(
            WindowedValues.of("foo", now, futureFutureWindow, paneInfo),
            WindowedValues.of("foo", now, futureWindow, paneInfo),
            WindowedValues.of("foo", now, centerWindow, paneInfo),
            WindowedValues.of("foo", now, pastWindow, paneInfo)));

    assertThat(value, not(instanceOf(WindowedValues.SingleWindowedValue.class)));
  }

  @Test
  public void testSingleWindowedValueInGlobalWindow() {
    WindowedValue<Integer> value =
        WindowedValues.of(1, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
    assertThat(value, instanceOf(WindowedValues.SingleWindowedValue.class));
    assertThat(
        ((WindowedValues.SingleWindowedValue) value).getWindow(), equalTo(GlobalWindow.INSTANCE));
  }

  @Test
  public void testSingleWindowedValueInFixedWindow() {
    Instant now = Instant.now();
    BoundedWindow w = new IntervalWindow(now, now.plus(Duration.millis(1)));
    WindowedValue<Integer> value = WindowedValues.of(1, now, w, PaneInfo.NO_FIRING);
    assertThat(value, instanceOf(WindowedValues.SingleWindowedValue.class));
    assertThat(((WindowedValues.SingleWindowedValue) value).getWindow(), equalTo(w));
  }

  @Test
  public void testFullWindowedValueCoderSingleIntervalWindowAndParity() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    WindowedValue<String> single = WindowedValues.of("hello", ts, w1, PaneInfo.NO_FIRING);
    byte[] singleBytes = CoderUtils.encodeToByteArray(intervalCoder, single);
    byte[] singleNestedBytes =
        CoderUtils.encodeToByteArray(intervalCoder, single, Coder.Context.NESTED);

    // Byte-for-byte wire parity with legacy InstantCoder + CollectionCoder + PaneInfoCoder + value.
    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, expected);
    CollectionCoder.of(IntervalWindow.getCoder()).encode(Collections.singletonList(w1), expected);
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, expected);
    StringUtf8Coder.of().encode("hello", expected);
    Assert.assertArrayEquals(expected.toByteArray(), singleNestedBytes);

    WindowedValue<String> decodedSingle =
        CoderUtils.decodeFromByteArray(intervalCoder, singleBytes);
    assertThat(decodedSingle, equalTo(single));
    assertThat(decodedSingle, instanceOf(WindowedValues.SingleWindowedValue.class));
    Assert.assertEquals(singleNestedBytes.length, measureByteSize(intervalCoder, single));
  }

  @Test
  public void testFullWindowedValueCoderMultipleWindows() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    WindowedValue<String> multi =
        WindowedValues.of("world", ts, Arrays.asList(w1, w2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
    byte[] multiBytes = CoderUtils.encodeToByteArray(intervalCoder, multi);
    WindowedValue<String> decodedMulti = CoderUtils.decodeFromByteArray(intervalCoder, multiBytes);
    assertThat(decodedMulti, equalTo(multi));
    Assert.assertEquals(
        CoderUtils.encodeToByteArray(intervalCoder, multi, Coder.Context.NESTED).length,
        measureByteSize(intervalCoder, multi));
  }

  @Test
  public void testFullWindowedValueCoderSingleGlobalWindowAndParity() throws Exception {
    Coder<WindowedValue<String>> globalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    WindowedValue<String> globalVal = WindowedValues.valueInGlobalWindow("g");
    byte[] globalBytes = CoderUtils.encodeToByteArray(globalCoder, globalVal);
    byte[] globalNestedBytes =
        CoderUtils.encodeToByteArray(globalCoder, globalVal, Coder.Context.NESTED);

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    InstantCoder.of().encode(BoundedWindow.TIMESTAMP_MIN_VALUE, expected);
    CollectionCoder.of(GlobalWindow.Coder.INSTANCE)
        .encode(Collections.singletonList(GlobalWindow.INSTANCE), expected);
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, expected);
    StringUtf8Coder.of().encode("g", expected);
    Assert.assertArrayEquals(expected.toByteArray(), globalNestedBytes);

    WindowedValue<String> decodedGlobal = CoderUtils.decodeFromByteArray(globalCoder, globalBytes);
    assertThat(decodedGlobal, equalTo(globalVal));
    Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, decodedGlobal.getTimestamp());
    Assert.assertEquals(globalNestedBytes.length, measureByteSize(globalCoder, globalVal));
  }

  @Test
  public void testFullWindowedValueCoderCustomNonSingleWindowedValueFallback() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    WindowedValue<String> delegate = WindowedValues.of("custom", ts, w1, PaneInfo.NO_FIRING);
    WindowedValue<String> nonSingleWrapper = new CustomWindowedValue<>(delegate);

    Assert.assertNull(WindowedValues.singleWindowOrNull(nonSingleWrapper));
    byte[] wrappedBytes = CoderUtils.encodeToByteArray(intervalCoder, nonSingleWrapper);
    byte[] directBytes = CoderUtils.encodeToByteArray(intervalCoder, delegate);
    Assert.assertArrayEquals(directBytes, wrappedBytes);
    Assert.assertEquals(
        measureByteSize(intervalCoder, delegate), measureByteSize(intervalCoder, nonSingleWrapper));
  }

  @Test
  public void testFullWindowedValueCoderSingleWindowWithMetadataSupported() throws Exception {
    WindowedValues.WindowedValueCoder.setMetadataSupported();
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    // Round-trip with active OpenTelemetry span context.
    Context activeContext = newActiveSpanContext("ff000000000000000000000000000041");
    WindowedValue<String> withMeta =
        WindowedValues.of(
            "traced",
            ts,
            w1,
            PaneInfo.ON_TIME_AND_ONLY_FIRING,
            null,
            null,
            CausedByDrain.CAUSED_BY_DRAIN,
            activeContext,
            ValueKind.UPDATE_BEFORE);
    WindowedValue<String> decodedWithMeta =
        CoderUtils.decodeFromByteArray(
            intervalCoder, CoderUtils.encodeToByteArray(intervalCoder, withMeta));
    assertThat(decodedWithMeta, instanceOf(WindowedValues.SingleWindowedValue.class));
    Assert.assertEquals("traced", decodedWithMeta.getValue());
    Assert.assertEquals(ts, decodedWithMeta.getTimestamp());
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(decodedWithMeta));
    Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, decodedWithMeta.causedByDrain());
    Assert.assertEquals(ValueKind.UPDATE_BEFORE, decodedWithMeta.getValueKind());
    Assert.assertNotNull(decodedWithMeta.getOpenTelemetryContext());
    Assert.assertEquals(
        "ff000000000000000000000000000041",
        Span.fromContext(decodedWithMeta.getOpenTelemetryContext()).getSpanContext().getTraceId());

    // Round-trip without active context keeps getOpenTelemetryContext() normalized to null.
    WindowedValue<String> noTrace =
        WindowedValues.of(
            "plain",
            ts,
            w1,
            PaneInfo.NO_FIRING,
            null,
            null,
            CausedByDrain.NORMAL,
            null,
            ValueKind.DELETE);
    WindowedValue<String> decodedNoTrace =
        CoderUtils.decodeFromByteArray(
            intervalCoder, CoderUtils.encodeToByteArray(intervalCoder, noTrace));
    Assert.assertEquals("plain", decodedNoTrace.getValue());
    Assert.assertEquals(CausedByDrain.NORMAL, decodedNoTrace.causedByDrain());
    Assert.assertEquals(ValueKind.DELETE, decodedNoTrace.getValueKind());
    Assert.assertNull(decodedNoTrace.getOpenTelemetryContext());
  }

  @Test
  public void testFullWindowedValueCoderZeroWindowsLegacyStream() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    ByteArrayOutputStream zeroWinOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, zeroWinOut);
    zeroWinOut.write(new byte[] {0, 0, 0, 0}); // numWindows == 0
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, zeroWinOut);
    StringUtf8Coder.of().encode("empty", zeroWinOut);
    WindowedValue<String> decodedZero =
        intervalCoder.decode(new ByteArrayInputStream(zeroWinOut.toByteArray()));
    assertThat(decodedZero.getValue(), equalTo("empty"));
    assertThat(decodedZero.getWindows(), equalTo(Collections.emptyList()));
  }

  @Test
  public void testFullWindowedValueCoderUnknownSizeChunkedWindows() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    // Multi-chunk unknown-size stream (-1, chunk 1: w1, chunk 2: w2, terminator: 0).
    ByteArrayOutputStream multiChunkOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, multiChunkOut);
    multiChunkOut.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    VarInt.encode(1L, multiChunkOut);
    IntervalWindow.getCoder().encode(w1, multiChunkOut);
    VarInt.encode(1L, multiChunkOut);
    IntervalWindow.getCoder().encode(w2, multiChunkOut);
    VarInt.encode(0L, multiChunkOut);
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, multiChunkOut);
    StringUtf8Coder.of().encode("multi-chunk", multiChunkOut);
    WindowedValue<String> decodedMulti =
        intervalCoder.decode(new ByteArrayInputStream(multiChunkOut.toByteArray()));
    assertThat(decodedMulti.getValue(), equalTo("multi-chunk"));
    assertThat(decodedMulti.getWindows(), containsInAnyOrder(w1, w2));

    // Single-window unknown-size stream (-1, count=1, w1, terminator=0) -> SingleWindowedValue.
    ByteArrayOutputStream singleChunkOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, singleChunkOut);
    singleChunkOut.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    VarInt.encode(1L, singleChunkOut);
    IntervalWindow.getCoder().encode(w1, singleChunkOut);
    VarInt.encode(0L, singleChunkOut);
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, singleChunkOut);
    StringUtf8Coder.of().encode("single-chunk", singleChunkOut);
    WindowedValue<String> decodedSingle =
        intervalCoder.decode(new ByteArrayInputStream(singleChunkOut.toByteArray()));
    assertThat(decodedSingle, instanceOf(WindowedValues.SingleWindowedValue.class));
    assertThat(decodedSingle.getValue(), equalTo("single-chunk"));
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(decodedSingle));

    // Empty unknown-size stream (-1, terminator=0).
    ByteArrayOutputStream emptyChunkOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, emptyChunkOut);
    emptyChunkOut.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    VarInt.encode(0L, emptyChunkOut);
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, emptyChunkOut);
    StringUtf8Coder.of().encode("empty-chunk", emptyChunkOut);
    WindowedValue<String> decodedEmpty =
        intervalCoder.decode(new ByteArrayInputStream(emptyChunkOut.toByteArray()));
    assertThat(decodedEmpty.getValue(), equalTo("empty-chunk"));
    assertThat(decodedEmpty.getWindows(), equalTo(Collections.emptyList()));

    // Non-zero negative terminator throws IllegalStateException.
    ByteArrayOutputStream badTermOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, badTermOut);
    badTermOut.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    VarInt.encode(-2L, badTermOut);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> intervalCoder.decode(new ByteArrayInputStream(badTermOut.toByteArray())));
  }

  @Test
  public void testFullWindowedValueCoderTruncatedStreamsThrowEof() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    byte[] encodedTs = CoderUtils.encodeToByteArray(InstantCoder.of(), ts);
    for (int extraBytes = 0; extraBytes <= 3; extraBytes++) {
      byte[] truncated = new byte[encodedTs.length + extraBytes];
      System.arraycopy(encodedTs, 0, truncated, 0, encodedTs.length);
      Assert.assertThrows(
          EOFException.class, () -> intervalCoder.decode(new ByteArrayInputStream(truncated)));
      IllegalArgumentException wrapped =
          Assert.assertThrows(
              IllegalArgumentException.class,
              () -> CoderUtils.decodeFromByteArray(intervalCoder, truncated));
      assertThat(wrapped.getCause(), instanceOf(EOFException.class));
    }

    // Truncated unknown-size chunk stream (missing VarInt chunk count after -1 header).
    ByteArrayOutputStream truncatedChunk = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, truncatedChunk);
    truncatedChunk.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    byte[] truncatedChunkBytes = truncatedChunk.toByteArray();
    Assert.assertThrows(
        EOFException.class,
        () -> intervalCoder.decode(new ByteArrayInputStream(truncatedChunkBytes)));
  }

  @Test
  public void testFullWindowedValueCoderNullDecodedSingleWindowThrowsNpe() throws Exception {
    Coder<BoundedWindow> nullWindowCoder =
        new CustomCoder<BoundedWindow>() {
          @Override
          public void encode(BoundedWindow value, OutputStream outStream) {}

          @Override
          @SuppressWarnings("nullness")
          public BoundedWindow decode(InputStream inStream) {
            return null;
          }
        };
    Coder<WindowedValue<String>> coder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), nullWindowCoder);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InstantCoder.of().encode(new Instant(1_700_000_000_000L), out);
    out.write(new byte[] {0, 0, 0, 1}); // numWindows == 1
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, out);
    StringUtf8Coder.of().encode("x", out);
    NullPointerException ex =
        Assert.assertThrows(
            NullPointerException.class,
            () -> coder.decode(new ByteArrayInputStream(out.toByteArray())));
    assertThat(ex.getMessage(), equalTo("Decoded single window must not be null"));
  }

  @Test
  public void testOfWithCurrentContextOmitsRootAndReplacesTemplateContext() {
    Context templateContext = newActiveSpanContext("aa000000000000000000000000000001");
    Context activeContext = newActiveSpanContext("ff000000000000000000000000000041");
    Instant ts = new Instant(1_700_000_000_000L);
    Instant ts2 = ts.plus(Duration.standardSeconds(5));
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));
    PaneInfo early = PaneInfo.createPane(false, false, Timing.EARLY, 3, 1);

    WindowedValue<String> singleTemplate =
        WindowedValues.of(
            "in",
            ts,
            w1,
            early,
            "rid-1",
            42L,
            CausedByDrain.CAUSED_BY_DRAIN,
            templateContext,
            ValueKind.DELETE);

    try (Scope rootScope = Context.root().makeCurrent()) {
      WindowedValue<String> derived = WindowedValues.ofWithCurrentContext(singleTemplate, "out");
      Assert.assertEquals("out", derived.getValue());
      Assert.assertEquals(ts, derived.getTimestamp());
      Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(derived));
      Assert.assertEquals(early, derived.getPaneInfo());
      Assert.assertEquals("rid-1", derived.getRecordId());
      Assert.assertEquals(Long.valueOf(42L), derived.getRecordOffset());
      Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, derived.causedByDrain());
      Assert.assertEquals(ValueKind.DELETE, derived.getValueKind());
      // Template's active OTel context must NOT leak when Context.root() is current.
      Assert.assertNull(derived.getOpenTelemetryContext());

      WindowedValue<@Nullable String> withNullValue =
          WindowedValues.ofWithCurrentContext(singleTemplate, null, ts2);
      Assert.assertNull(withNullValue.getValue());
      Assert.assertEquals(ts2, withNullValue.getTimestamp());
      Assert.assertNull(withNullValue.getOpenTelemetryContext());

      WindowedValue<String> built =
          WindowedValues.<String>builder()
              .setValue("b")
              .setTimestamp(BoundedWindow.TIMESTAMP_MIN_VALUE)
              .setWindow(GlobalWindow.INSTANCE)
              .setPaneInfo(PaneInfo.NO_FIRING)
              .build();
      Assert.assertNull(built.getOpenTelemetryContext());
    }

    try (Scope activeScope = activeContext.makeCurrent()) {
      WindowedValue<String> derived = WindowedValues.ofWithCurrentContext(singleTemplate, "out");
      Assert.assertSame(activeContext, derived.getOpenTelemetryContext());

      WindowedValue<String> derivedWithTs =
          WindowedValues.ofWithCurrentContext(singleTemplate, "out2", ts2);
      Assert.assertSame(activeContext, derivedWithTs.getOpenTelemetryContext());
      Assert.assertEquals(ts2, derivedWithTs.getTimestamp());

      WindowedValue<String> explicit5Arg =
          WindowedValues.ofWithCurrentContext(
              singleTemplate, "out5", ts2, Collections.singletonList(w2), PaneInfo.NO_FIRING);
      Assert.assertSame(activeContext, explicit5Arg.getOpenTelemetryContext());
      Assert.assertEquals(w2, WindowedValues.singleWindowOrNull(explicit5Arg));
      Assert.assertEquals("rid-1", explicit5Arg.getRecordId());
      Assert.assertEquals(Long.valueOf(42L), explicit5Arg.getRecordOffset());
      Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, explicit5Arg.causedByDrain());
      Assert.assertEquals(ValueKind.DELETE, explicit5Arg.getValueKind());

      WindowedValue<String> explicit6Arg =
          WindowedValues.ofWithCurrentContext(
              singleTemplate,
              "out6",
              ts2,
              Arrays.asList(w1, w2),
              PaneInfo.ON_TIME_AND_ONLY_FIRING,
              ValueKind.UPDATE_AFTER);
      Assert.assertSame(activeContext, explicit6Arg.getOpenTelemetryContext());
      Assert.assertEquals(ValueKind.UPDATE_AFTER, explicit6Arg.getValueKind());
      Assert.assertEquals("rid-1", explicit6Arg.getRecordId());
      assertThat(explicit6Arg.getWindows(), containsInAnyOrder(w1, w2));

      WindowedValue<String> direct8ArgSingle =
          WindowedValues.ofWithCurrentContext(
              "d1",
              ts2,
              w1,
              early,
              "rid-8",
              7L,
              CausedByDrain.CAUSED_BY_DRAIN,
              ValueKind.UPDATE_BEFORE);
      Assert.assertSame(activeContext, direct8ArgSingle.getOpenTelemetryContext());
      Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(direct8ArgSingle));
      Assert.assertEquals(ValueKind.UPDATE_BEFORE, direct8ArgSingle.getValueKind());

      WindowedValue<String> direct8ArgMulti =
          WindowedValues.ofWithCurrentContext(
              "d2",
              ts2,
              Arrays.asList(w1, w2),
              early,
              null,
              null,
              CausedByDrain.NORMAL,
              ValueKind.INSERT);
      Assert.assertSame(activeContext, direct8ArgMulti.getOpenTelemetryContext());
      assertThat(direct8ArgMulti.getWindows(), containsInAnyOrder(w1, w2));
    }

    // WindowedValues.withValue preserves template's own context (contrast with
    // ofWithCurrentContext).
    try (Scope rootScope = Context.root().makeCurrent()) {
      WindowedValue<Integer> preserved = WindowedValues.withValue(singleTemplate, 99);
      Assert.assertSame(templateContext, preserved.getOpenTelemetryContext());
      Assert.assertEquals(Integer.valueOf(99), preserved.getValue());
      Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(preserved));

      WindowedValue<String> customWrapper = new CustomWindowedValue<>(singleTemplate);
      WindowedValue<Integer> customPreserved = WindowedValues.withValue(customWrapper, 100);
      Assert.assertSame(templateContext, customPreserved.getOpenTelemetryContext());
      Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(customPreserved));
    }
  }

  @Test
  public void testSingleWindowOrNullAndGlobalTimestampTransitions() {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));

    WindowedValue<String> gMin = WindowedValues.valueInGlobalWindow("g0");
    Assert.assertEquals(GlobalWindow.INSTANCE, WindowedValues.singleWindowOrNull(gMin));
    WindowedValue<String> gTs = WindowedValues.ofWithCurrentContext(gMin, "g1", ts);
    Assert.assertEquals(ts, gTs.getTimestamp());
    Assert.assertEquals(GlobalWindow.INSTANCE, WindowedValues.singleWindowOrNull(gTs));
    // Timestamp with equal millis but distinct Instant object transitions back to MIN
    // representation.
    WindowedValue<String> gBackToMin =
        WindowedValues.ofWithCurrentContext(
            gTs, "g2", new Instant(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));
    Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, gBackToMin.getTimestamp());
    Assert.assertEquals(GlobalWindow.INSTANCE, WindowedValues.singleWindowOrNull(gBackToMin));

    WindowedValue<String> multi =
        WindowedValues.of("m0", ts, Arrays.asList(w1, w2), PaneInfo.NO_FIRING);
    Assert.assertNull(WindowedValues.singleWindowOrNull(multi));
    WindowedValue<String> multiCopy = WindowedValues.ofWithCurrentContext(multi, "m1");
    assertThat(multiCopy.getWindows(), containsInAnyOrder(w1, w2));

    // Builder template (single-window vs unset vs multi-window).
    WindowedValues.Builder<String> singleBuilder =
        WindowedValues.<String>builder()
            .setValue("b")
            .setTimestamp(ts)
            .setWindow(w1)
            .setPaneInfo(PaneInfo.NO_FIRING);
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(singleBuilder));
    WindowedValue<Integer> fromBuilder = WindowedValues.ofWithCurrentContext(singleBuilder, 123);
    Assert.assertEquals(Integer.valueOf(123), fromBuilder.getValue());
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(fromBuilder));
    assertThat(singleBuilder.getWindows(), containsInAnyOrder(w1));
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> ((Collection<BoundedWindow>) singleBuilder.getWindows()).add(w2));

    WindowedValues.Builder<String> emptyBuilder = WindowedValues.builder();
    Assert.assertNull(WindowedValues.singleWindowOrNull(emptyBuilder));

    // Non-List singleton collection (Set) fallback in WindowedValues.of().
    WindowedValue<String> fromSet =
        WindowedValues.of("set", ts, Collections.singleton(w1), PaneInfo.NO_FIRING);
    assertThat(fromSet, instanceOf(WindowedValues.SingleWindowedValue.class));
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(fromSet));

    // Null windows throws IllegalArgumentException.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> WindowedValues.of("x", ts, (Collection<BoundedWindow>) null, PaneInfo.NO_FIRING));

    // Empty-window template decoded via zero-window stream throws IllegalArgumentException.
    WindowedValue<String> emptyTemplate =
        new CustomWindowedValue<String>(gMin) {
          @Override
          public Collection<? extends BoundedWindow> getWindows() {
            return Collections.emptyList();
          }
        };
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> WindowedValues.ofWithCurrentContext(emptyTemplate, "out"));
  }

  @Test
  public void testBuilderWithTemplateDoesNotReadTemplateValue() {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    WindowedValue<String> base =
        WindowedValues.of(
            "secret",
            ts,
            w1,
            PaneInfo.ON_TIME_AND_ONLY_FIRING,
            "rid",
            9L,
            CausedByDrain.CAUSED_BY_DRAIN,
            null,
            ValueKind.UPDATE_BEFORE);
    AtomicBoolean valueRead = new AtomicBoolean(false);
    WindowedValue<String> guardingTemplate =
        new CustomWindowedValue<String>(base) {
          @Override
          public String getValue() {
            valueRead.set(true);
            throw new AssertionError("template.getValue() must not be called");
          }
        };

    WindowedValue<Integer> built = WindowedValues.builder(guardingTemplate, 77).build();
    Assert.assertFalse(valueRead.get());
    Assert.assertEquals(Integer.valueOf(77), built.getValue());
    Assert.assertEquals(ts, built.getTimestamp());
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(built));
    Assert.assertEquals("rid", built.getRecordId());
    Assert.assertEquals(Long.valueOf(9L), built.getRecordOffset());
    Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, built.causedByDrain());
    Assert.assertEquals(ValueKind.UPDATE_BEFORE, built.getValueKind());

    // Null new value on Builder.
    WindowedValue<@Nullable Integer> builtNull =
        WindowedValues.builder(guardingTemplate, (Integer) null).build();
    Assert.assertNull(builtNull.getValue());
  }

  private static Context newActiveSpanContext(String traceId) {
    return Context.root()
        .with(
            Span.wrap(
                SpanContext.create(
                    traceId,
                    "ff00000000000041",
                    TraceFlags.getSampled(),
                    TraceState.getDefault())));
  }

  private static class CustomWindowedValue<T> implements WindowedValue<T> {
    private final WindowedValue<T> delegate;

    CustomWindowedValue(WindowedValue<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public T getValue() {
      return delegate.getValue();
    }

    @Override
    public Instant getTimestamp() {
      return delegate.getTimestamp();
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return delegate.getWindows();
    }

    @Override
    public PaneInfo getPaneInfo() {
      return delegate.getPaneInfo();
    }

    @Override
    public @Nullable String getRecordId() {
      return delegate.getRecordId();
    }

    @Override
    public @Nullable Context getOpenTelemetryContext() {
      return delegate.getOpenTelemetryContext();
    }

    @Override
    public @Nullable Long getRecordOffset() {
      return delegate.getRecordOffset();
    }

    @Override
    public CausedByDrain causedByDrain() {
      return delegate.causedByDrain();
    }

    @Override
    public ValueKind getValueKind() {
      return delegate.getValueKind();
    }

    @Override
    public Iterable<? extends WindowedValue<T>> explodeWindows() {
      return delegate.explodeWindows();
    }

    @Override
    public <OtherT> WindowedValue<OtherT> withValue(OtherT value) {
      return delegate.withValue(value);
    }
  }

  private static <T> long measureByteSize(Coder<T> coder, T value) throws Exception {
    final long[] total = new long[1];
    ElementByteSizeObserver observer =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementByteSize) {
            total[0] += elementByteSize;
          }
        };
    coder.registerByteSizeObserver(value, observer);
    observer.advance();
    return total[0];
  }
}
