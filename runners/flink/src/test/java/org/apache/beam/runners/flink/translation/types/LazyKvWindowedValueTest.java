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
package org.apache.beam.runners.flink.translation.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.adapter.FlinkKey;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkKeyUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToFlinkKeyKeySelector;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link LazyKvWindowedValue} and the lazy mode of {@link LengthPrefixedCoderTypeSerializer}.
 */
public class LazyKvWindowedValueTest implements Serializable {

  private static final AtomicInteger VALUE_CODER_CALLS = new AtomicInteger();

  /** A coder for the KV values that counts how often it is invoked. */
  private static class CountingValueCoder extends AtomicCoder<List<String>> {
    @Override
    public void encode(List<String> value, OutputStream outStream) throws IOException {
      VALUE_CODER_CALLS.incrementAndGet();
      VarIntCoder.of().encode(value.size(), outStream);
      for (String s : value) {
        StringUtf8Coder.of().encode(s, outStream);
      }
    }

    @Override
    public List<String> decode(InputStream inStream) throws IOException {
      VALUE_CODER_CALLS.incrementAndGet();
      int size = VarIntCoder.of().decode(inStream);
      List<String> value = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        value.add(StringUtf8Coder.of().decode(inStream));
      }
      return value;
    }
  }

  private static final KvCoder<String, List<String>> KV_CODER =
      KvCoder.of(StringUtf8Coder.of(), new CountingValueCoder());

  private static final Coder<WindowedValue<KV<String, List<String>>>> GLOBAL_CODER =
      WindowedValues.getFullCoder(KV_CODER, GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<String, List<String>>>> INTERVAL_CODER =
      WindowedValues.getFullCoder(KV_CODER, IntervalWindow.getCoder());

  private static final WindowedValue<KV<String, List<String>>> GLOBAL_VALUE =
      WindowedValues.valueInGlobalWindow(KV.of("key", Arrays.asList("a", "b", "c")));

  private static final WindowedValue<KV<String, List<String>>> INTERVAL_VALUE =
      WindowedValues.of(
          KV.of("other-key", Arrays.asList("d")),
          new Instant(42),
          Arrays.asList(
              new IntervalWindow(new Instant(0), new Instant(100)),
              new IntervalWindow(new Instant(10), new Instant(110))),
          PaneInfo.ON_TIME_AND_ONLY_FIRING);

  @Before
  public void resetCounter() {
    VALUE_CODER_CALLS.set(0);
  }

  private static <T> byte[] serialize(LengthPrefixedCoderTypeSerializer<T> serializer, T value)
      throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(64);
    serializer.serialize(value, out);
    return out.getCopyOfBuffer();
  }

  private static LazyKvWindowedValue<String, List<String>> lazy(
      Coder<WindowedValue<KV<String, List<String>>>> coder,
      WindowedValue<KV<String, List<String>>> value)
      throws IOException {
    LengthPrefixedCoderTypeSerializer<WindowedValue<KV<String, List<String>>>> serializer =
        new LengthPrefixedCoderTypeSerializer<>(coder, false, true);
    WindowedValue<KV<String, List<String>>> result =
        serializer.deserialize(new DataInputDeserializer(serialize(serializer, value)));
    assertThat(result, instanceOf(LazyKvWindowedValue.class));
    VALUE_CODER_CALLS.set(0);
    return (LazyKvWindowedValue<String, List<String>>) result;
  }

  @Test
  public void exposesWindowingInformationWithoutDecodingTheValue() throws IOException {
    for (WindowedValue<KV<String, List<String>>> expected :
        Arrays.asList(GLOBAL_VALUE, INTERVAL_VALUE)) {
      Coder<WindowedValue<KV<String, List<String>>>> coder =
          expected == GLOBAL_VALUE ? GLOBAL_CODER : INTERVAL_CODER;
      LazyKvWindowedValue<String, List<String>> lazy = lazy(coder, expected);

      assertThat(lazy.getTimestamp(), is(expected.getTimestamp()));
      assertThat(lazy.getWindows(), is(expected.getWindows()));
      assertThat(lazy.getPaneInfo(), is(expected.getPaneInfo()));
      assertThat(lazy.causedByDrain(), is(expected.causedByDrain()));
      assertThat(lazy.getValueKind(), is(expected.getValueKind()));
      assertThat(VALUE_CODER_CALLS.get(), is(0));
    }
  }

  @Test
  public void decodesTheValueOnce() throws IOException {
    LazyKvWindowedValue<String, List<String>> lazy = lazy(GLOBAL_CODER, GLOBAL_VALUE);
    assertThat(lazy.getValue(), is(GLOBAL_VALUE.getValue()));
    assertThat(lazy.getValue(), is(GLOBAL_VALUE.getValue()));
    assertThat(VALUE_CODER_CALLS.get(), is(1));
  }

  @Test
  public void encodedKeyMatchesFlinkKeyWithoutDecodingTheValue() throws IOException {
    LazyKvWindowedValue<String, List<String>> lazy = lazy(GLOBAL_CODER, GLOBAL_VALUE);
    byte[] expected = FlinkKeyUtils.encodeKey("key", StringUtf8Coder.of()).array();

    assertArrayEquals(expected, lazy.getEncodedKey());
    assertThat(VALUE_CODER_CALLS.get(), is(0));

    // Same result once the value has been decoded.
    lazy.getValue();
    assertArrayEquals(expected, lazy.getEncodedKey());
  }

  @Test
  public void keySelectorReturnsTheSameKeyForLazyAndEagerValues() throws IOException {
    KvToFlinkKeyKeySelector<String, List<String>> selector =
        new KvToFlinkKeyKeySelector<>(StringUtf8Coder.of());
    LazyKvWindowedValue<String, List<String>> lazy = lazy(GLOBAL_CODER, GLOBAL_VALUE);

    FlinkKey lazyKey = selector.getKey(lazy);
    assertThat(VALUE_CODER_CALLS.get(), is(0));
    assertThat(lazyKey, is(selector.getKey(GLOBAL_VALUE)));
    assertThat(lazyKey, is(FlinkKey.of(ByteBuffer.wrap(lazy.getEncodedKey()))));
  }

  @Test
  public void equalsTheEquivalentDecodedValue() throws IOException {
    LazyKvWindowedValue<String, List<String>> lazy = lazy(INTERVAL_CODER, INTERVAL_VALUE);
    assertThat(lazy.equals(INTERVAL_VALUE), is(true));
    assertThat(lazy.hashCode(), is(INTERVAL_VALUE.hashCode()));
    assertThat(lazy.equals(lazy(INTERVAL_CODER, INTERVAL_VALUE)), is(true));
    assertThat(lazy.equals(GLOBAL_VALUE), is(false));
  }

  @Test
  public void explodesWindowsAndReplacesValues() throws IOException {
    LazyKvWindowedValue<String, List<String>> lazy = lazy(INTERVAL_CODER, INTERVAL_VALUE);

    List<WindowedValue<KV<String, List<String>>>> exploded = new ArrayList<>();
    lazy.explodeWindows().forEach(exploded::add);
    List<WindowedValue<KV<String, List<String>>>> expected = new ArrayList<>();
    INTERVAL_VALUE.explodeWindows().forEach(expected::add);
    assertThat(exploded, is(expected));

    LazyKvWindowedValue<String, List<String>> other = lazy(INTERVAL_CODER, INTERVAL_VALUE);
    assertThat(other.withValue(7), is(INTERVAL_VALUE.withValue(7)));
    assertThat(VALUE_CODER_CALLS.get(), is(0));
  }

  @Test
  public void reserializesUntouchedValuesWithoutTheValueCoder() throws IOException {
    LengthPrefixedCoderTypeSerializer<WindowedValue<KV<String, List<String>>>> serializer =
        new LengthPrefixedCoderTypeSerializer<>(INTERVAL_CODER, false, true);
    byte[] original = serialize(serializer, INTERVAL_VALUE);

    WindowedValue<KV<String, List<String>>> lazy =
        serializer.deserialize(new DataInputDeserializer(original));
    VALUE_CODER_CALLS.set(0);
    byte[] reserialized = serialize(serializer, lazy);

    assertThat(VALUE_CODER_CALLS.get(), is(0));
    assertArrayEquals(original, reserialized);
    // Lazy and eager serializers write the same bytes.
    assertArrayEquals(
        original, serialize(new LengthPrefixedCoderTypeSerializer<>(INTERVAL_CODER, false), lazy));
  }

  @Test
  public void reserializesMutatedValues() throws IOException {
    LengthPrefixedCoderTypeSerializer<WindowedValue<KV<String, List<String>>>> serializer =
        new LengthPrefixedCoderTypeSerializer<>(GLOBAL_CODER, false, true);
    WindowedValue<KV<String, List<String>>> lazy =
        serializer.deserialize(new DataInputDeserializer(serialize(serializer, GLOBAL_VALUE)));

    lazy.getValue().getValue().add("added");
    WindowedValue<KV<String, List<String>>> result =
        serializer.deserialize(new DataInputDeserializer(serialize(serializer, lazy)));

    assertThat(result.getValue().getValue(), is(Arrays.asList("a", "b", "c", "added")));
  }

  @Test
  public void readsLazyAndEagerRecordsBackToBack() throws IOException {
    LengthPrefixedCoderTypeSerializer<WindowedValue<KV<String, List<String>>>> serializer =
        new LengthPrefixedCoderTypeSerializer<>(INTERVAL_CODER, false, true);
    LazyKvWindowedValue<String, List<String>> touched = lazy(INTERVAL_CODER, INTERVAL_VALUE);
    touched.getValue();

    DataOutputSerializer out = new DataOutputSerializer(64);
    serializer.serialize(INTERVAL_VALUE, out);
    serializer.serialize(lazy(INTERVAL_CODER, INTERVAL_VALUE), out);
    serializer.serialize(touched, out);

    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    for (int i = 0; i < 3; i++) {
      assertThat(serializer.deserialize(in).equals(INTERVAL_VALUE), is(true));
    }
    assertThat(in.available(), is(0));
  }

  @Test
  public void rejectsUnsupportedCoders() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LengthPrefixedCoderTypeSerializer<>(StringUtf8Coder.of(), false, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LengthPrefixedCoderTypeSerializer<>(
                WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE),
                false,
                true));
  }

  @Test
  public void lazyAndEagerSerializersAreNotEqual() {
    assertThat(
        new LengthPrefixedCoderTypeSerializer<>(GLOBAL_CODER, false, true)
            .equals(new LengthPrefixedCoderTypeSerializer<>(GLOBAL_CODER, false)),
        is(false));
    assertThat(
        new LengthPrefixedCoderTypeSerializer<>(GLOBAL_CODER, false, true)
            .duplicate()
            .equals(new LengthPrefixedCoderTypeSerializer<>(GLOBAL_CODER, false, true)),
        is(true));
  }

  private static final AtomicInteger WINDOW_CODER_CALLS = new AtomicInteger();

  /** An {@link IntervalWindow} coder that counts how often it is invoked. */
  private static class CountingWindowCoder extends AtomicCoder<IntervalWindow> {
    @Override
    public void encode(IntervalWindow value, OutputStream outStream) throws IOException {
      WINDOW_CODER_CALLS.incrementAndGet();
      IntervalWindow.getCoder().encode(value, outStream);
    }

    @Override
    public IntervalWindow decode(InputStream inStream) throws IOException {
      WINDOW_CODER_CALLS.incrementAndGet();
      return IntervalWindow.getCoder().decode(inStream);
    }
  }

  @Test
  public void movesRecordsWithoutDecodingTheirWindows() throws IOException {
    Coder<WindowedValue<KV<String, List<String>>>> coder =
        WindowedValues.getFullCoder(KV_CODER, new CountingWindowCoder());
    LengthPrefixedCoderTypeSerializer<WindowedValue<KV<String, List<String>>>> serializer =
        new LengthPrefixedCoderTypeSerializer<>(coder, false, true);
    byte[] original = serialize(serializer, INTERVAL_VALUE);

    WINDOW_CODER_CALLS.set(0);
    VALUE_CODER_CALLS.set(0);
    WindowedValue<KV<String, List<String>>> lazy =
        serializer.deserialize(new DataInputDeserializer(original));
    byte[] moved = serialize(serializer, lazy);

    assertArrayEquals(original, moved);
    assertThat(WINDOW_CODER_CALLS.get(), is(0));
    assertThat(VALUE_CODER_CALLS.get(), is(0));

    // The key can be read after windows of variable size.
    assertArrayEquals(
        FlinkKeyUtils.encodeKey("other-key", StringUtf8Coder.of()).array(),
        ((LazyKvWindowedValue<String, List<String>>) lazy).getEncodedKey());
    assertThat(VALUE_CODER_CALLS.get(), is(0));
    assertThat(
        new ArrayList<>(lazy.getWindows()), is(new ArrayList<>(INTERVAL_VALUE.getWindows())));
    assertThat(lazy.getValue(), is(INTERVAL_VALUE.getValue()));
  }

  @Test
  public void typeInformationUsesLazyValuesOnlyInBatchForSupportedCoders() {
    assertThat(isLazy(GLOBAL_CODER, false), is(true));
    assertThat(isLazy(GLOBAL_CODER, true), is(false));
    // Unsupported coders fall back to an eager serializer instead of failing.
    assertThat(isLazy(StringUtf8Coder.of(), false), is(false));
  }

  private static <T> boolean isLazy(Coder<T> coder, boolean streaming) {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setStreaming(streaming);
    return CoderTypeSerializer.forTypeInformation(
            coder, new SerializablePipelineOptions(options), true)
        .isLazyKvValues();
  }
}
