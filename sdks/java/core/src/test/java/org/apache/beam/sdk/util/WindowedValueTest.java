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
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
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
    Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, value.causedByDrain());
    Assert.assertNotNull(value.getOpenTelemetryContext());
    Assert.assertEquals(ValueKind.DELETE, value.getValueKind());
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
  public void testFullWindowedValueCoderSingleMultiZeroAndChunkedWindows() throws Exception {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));
    Coder<WindowedValue<String>> intervalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    // Single IntervalWindow round-trip + byte size observer check.
    WindowedValue<String> single = WindowedValues.of("hello", ts, w1, PaneInfo.NO_FIRING);
    byte[] singleBytes = CoderUtils.encodeToByteArray(intervalCoder, single);
    WindowedValue<String> decodedSingle =
        CoderUtils.decodeFromByteArray(intervalCoder, singleBytes);
    assertThat(decodedSingle, equalTo(single));
    assertThat(decodedSingle, instanceOf(WindowedValues.SingleWindowedValue.class));
    Assert.assertEquals(
        CoderUtils.encodeToByteArray(intervalCoder, single, Coder.Context.NESTED).length,
        measureByteSize(intervalCoder, single));

    // Multiple windows round-trip + byte size observer check.
    WindowedValue<String> multi =
        WindowedValues.of("world", ts, Arrays.asList(w1, w2), PaneInfo.ON_TIME_AND_ONLY_FIRING);
    byte[] multiBytes = CoderUtils.encodeToByteArray(intervalCoder, multi);
    WindowedValue<String> decodedMulti = CoderUtils.decodeFromByteArray(intervalCoder, multiBytes);
    assertThat(decodedMulti, equalTo(multi));
    Assert.assertEquals(
        CoderUtils.encodeToByteArray(intervalCoder, multi, Coder.Context.NESTED).length,
        measureByteSize(intervalCoder, multi));

    // Single GlobalWindow round-trip + byte size observer check.
    Coder<WindowedValue<String>> globalCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    WindowedValue<String> globalVal = WindowedValues.valueInGlobalWindow("g");
    byte[] globalBytes = CoderUtils.encodeToByteArray(globalCoder, globalVal);
    WindowedValue<String> decodedGlobal = CoderUtils.decodeFromByteArray(globalCoder, globalBytes);
    assertThat(decodedGlobal, equalTo(globalVal));
    Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, decodedGlobal.getTimestamp());
    Assert.assertEquals(
        CoderUtils.encodeToByteArray(globalCoder, globalVal, Coder.Context.NESTED).length,
        measureByteSize(globalCoder, globalVal));

    // Zero windows stream (compatibility with legacy no-window encoding).
    ByteArrayOutputStream zeroWinOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, zeroWinOut);
    zeroWinOut.write(new byte[] {0, 0, 0, 0}); // numWindows == 0
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, zeroWinOut);
    StringUtf8Coder.of().encode("empty", zeroWinOut);
    WindowedValue<String> decodedZero =
        intervalCoder.decode(new ByteArrayInputStream(zeroWinOut.toByteArray()));
    assertThat(decodedZero.getValue(), equalTo("empty"));
    assertThat(decodedZero.getWindows(), equalTo(Collections.emptyList()));

    // Unknown-size chunked windows stream (-1 prefix, VarInt count=2, terminator=0).
    ByteArrayOutputStream chunkedOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, chunkedOut);
    chunkedOut.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    VarInt.encode(2L, chunkedOut);
    IntervalWindow.getCoder().encode(w1, chunkedOut);
    IntervalWindow.getCoder().encode(w2, chunkedOut);
    VarInt.encode(0L, chunkedOut);
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, chunkedOut);
    StringUtf8Coder.of().encode("chunked", chunkedOut);
    WindowedValue<String> decodedChunked =
        intervalCoder.decode(new ByteArrayInputStream(chunkedOut.toByteArray()));
    assertThat(decodedChunked.getValue(), equalTo("chunked"));
    assertThat(decodedChunked.getWindows(), containsInAnyOrder(w1, w2));

    // Non-zero negative terminator in unknown-size stream throws IllegalStateException.
    ByteArrayOutputStream badTermOut = new ByteArrayOutputStream();
    InstantCoder.of().encode(ts, badTermOut);
    badTermOut.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    VarInt.encode(-2L, badTermOut);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> intervalCoder.decode(new ByteArrayInputStream(badTermOut.toByteArray())));

    // Truncated IntervalWindowCoder stream throws CoderException.
    byte[] encodedInterval = CoderUtils.encodeToByteArray(IntervalWindow.getCoder(), w1);
    byte[] truncated = Arrays.copyOf(encodedInterval, 8); // timestamp only, missing duration
    Assert.assertThrows(
        CoderException.class,
        () -> CoderUtils.decodeFromByteArray(IntervalWindow.getCoder(), truncated));
  }

  @Test
  public void testOfWithCurrentContextOmitsRootAndPreservesActiveContext() {
    WindowedValue<String> template = WindowedValues.valueInGlobalWindow("in");
    try (Scope rootScope = Context.root().makeCurrent()) {
      WindowedValue<String> derived = WindowedValues.ofWithCurrentContext(template, "out");
      Assert.assertNull(derived.getOpenTelemetryContext());
      WindowedValue<String> built =
          WindowedValues.<String>builder()
              .setValue("b")
              .setTimestamp(BoundedWindow.TIMESTAMP_MIN_VALUE)
              .setWindow(GlobalWindow.INSTANCE)
              .setPaneInfo(PaneInfo.NO_FIRING)
              .build();
      Assert.assertNull(built.getOpenTelemetryContext());
    }

    Context activeContext =
        Context.root()
            .with(
                Span.wrap(
                    SpanContext.create(
                        "ff000000000000000000000000000041",
                        "ff00000000000041",
                        TraceFlags.getSampled(),
                        TraceState.getDefault())));
    try (Scope activeScope = activeContext.makeCurrent()) {
      WindowedValue<String> derived = WindowedValues.ofWithCurrentContext(template, "out");
      Assert.assertSame(activeContext, derived.getOpenTelemetryContext());
    }
  }

  @Test
  public void testSingleWindowOrNullAndOfWithCurrentContextFastPaths() {
    Instant ts = new Instant(1_700_000_000_000L);
    IntervalWindow w1 = new IntervalWindow(ts, ts.plus(Duration.standardMinutes(1)));
    IntervalWindow w2 =
        new IntervalWindow(
            ts.plus(Duration.standardMinutes(1)), ts.plus(Duration.standardMinutes(2)));
    PaneInfo early = PaneInfo.createPane(true, false, Timing.EARLY, 3, 1);

    // ValueInGlobalWindow
    WindowedValue<String> gMin = WindowedValues.valueInGlobalWindow("g0");
    Assert.assertEquals(GlobalWindow.INSTANCE, WindowedValues.singleWindowOrNull(gMin));
    WindowedValue<String> gMinDerived = WindowedValues.ofWithCurrentContext(gMin, "g1", ts);
    Assert.assertEquals(ts, gMinDerived.getTimestamp());
    Assert.assertEquals(GlobalWindow.INSTANCE, WindowedValues.singleWindowOrNull(gMinDerived));
    WindowedValue<String> gMinBackToMin =
        WindowedValues.ofWithCurrentContext(gMinDerived, "g2", BoundedWindow.TIMESTAMP_MIN_VALUE);
    Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, gMinBackToMin.getTimestamp());

    // TimestampedValueInSingleWindow with full metadata
    WindowedValue<String> single =
        WindowedValues.of(
            "s0", ts, w1, early, "rid", 42L, CausedByDrain.CAUSED_BY_DRAIN, null, ValueKind.DELETE);
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(single));
    WindowedValue<String> singleCopy = WindowedValues.ofWithCurrentContext(single, "s1");
    Assert.assertEquals("s1", singleCopy.getValue());
    Assert.assertEquals(ts, singleCopy.getTimestamp());
    Assert.assertEquals(w1, WindowedValues.singleWindowOrNull(singleCopy));
    Assert.assertEquals(early, singleCopy.getPaneInfo());
    Assert.assertEquals("rid", singleCopy.getRecordId());
    Assert.assertEquals(Long.valueOf(42L), singleCopy.getRecordOffset());
    Assert.assertEquals(CausedByDrain.CAUSED_BY_DRAIN, singleCopy.causedByDrain());
    Assert.assertEquals(ValueKind.DELETE, singleCopy.getValueKind());

    // TimestampedValueInMultipleWindows
    WindowedValue<String> multi = WindowedValues.of("m0", ts, Arrays.asList(w1, w2), early);
    Assert.assertNull(WindowedValues.singleWindowOrNull(multi));
    WindowedValue<String> multiDerived = WindowedValues.ofWithCurrentContext(multi, "m1");
    Assert.assertEquals("m1", multiDerived.getValue());
    assertThat(multiDerived.getWindows(), containsInAnyOrder(w1, w2));

    // 6-arg ofWithCurrentContext with ValueKind override
    WindowedValue<String> kindOverride =
        WindowedValues.ofWithCurrentContext(
            single,
            "k",
            ts.plus(Duration.millis(10)),
            Collections.singletonList(w2),
            early,
            ValueKind.UPDATE_AFTER);
    Assert.assertEquals(ValueKind.UPDATE_AFTER, kindOverride.getValueKind());
    Assert.assertEquals(w2, WindowedValues.singleWindowOrNull(kindOverride));
    Assert.assertEquals("rid", kindOverride.getRecordId());
    Assert.assertEquals(Long.valueOf(42L), kindOverride.getRecordOffset());
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
