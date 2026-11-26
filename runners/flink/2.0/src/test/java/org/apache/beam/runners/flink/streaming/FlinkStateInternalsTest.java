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
package org.apache.beam.runners.flink.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsTest;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.adapter.FlinkKey;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlinkStateInternals}. This is based on {@link StateInternalsTest}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class FlinkStateInternalsTest extends StateInternalsTest {

  @Override
  protected StateInternals createStateInternals() {
    try {
      KeyedStateBackend<FlinkKey> keyedStateBackend = createStateBackend();
      return new FlinkStateInternals<>(
          keyedStateBackend,
          StringUtf8Coder.of(),
          IntervalWindow.getCoder(),
          new SerializablePipelineOptions(FlinkPipelineOptions.defaults()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testWatermarkHoldsPersistence() throws Exception {
    KeyedStateBackend<FlinkKey> keyedStateBackend = createStateBackend();
    FlinkStateInternals stateInternals =
        new FlinkStateInternals<>(
            keyedStateBackend,
            StringUtf8Coder.of(),
            IntervalWindow.getCoder(),
            new SerializablePipelineOptions(FlinkPipelineOptions.defaults()));

    StateTag<WatermarkHoldState> stateTag =
        StateTags.watermarkStateInternal("hold", TimestampCombiner.EARLIEST);
    WatermarkHoldState globalWindow = stateInternals.state(StateNamespaces.global(), stateTag);
    WatermarkHoldState fixedWindow =
        stateInternals.state(
            StateNamespaces.window(
                IntervalWindow.getCoder(), new IntervalWindow(new Instant(0), new Instant(10))),
            stateTag);

    Instant noHold = new Instant(Long.MAX_VALUE);
    assertThat(stateInternals.minWatermarkHoldMs(), is(noHold.getMillis()));

    Instant high = new Instant(10);
    globalWindow.add(high);
    assertThat(stateInternals.minWatermarkHoldMs(), is(high.getMillis()));

    Instant middle = new Instant(5);
    fixedWindow.add(middle);
    assertThat(stateInternals.minWatermarkHoldMs(), is(middle.getMillis()));

    Instant low = new Instant(1);
    globalWindow.add(low);
    assertThat(stateInternals.minWatermarkHoldMs(), is(low.getMillis()));

    // Try to overwrite with later hold (should not succeed)
    globalWindow.add(high);
    assertThat(stateInternals.minWatermarkHoldMs(), is(low.getMillis()));
    fixedWindow.add(high);
    assertThat(stateInternals.minWatermarkHoldMs(), is(low.getMillis()));

    // Watermark hold should be computed across all keys
    FlinkKey firstKey = keyedStateBackend.getCurrentKey();
    changeKey(keyedStateBackend);
    FlinkKey secondKey = keyedStateBackend.getCurrentKey();
    assertThat(firstKey, is(Matchers.not(secondKey)));
    assertThat(stateInternals.minWatermarkHoldMs(), is(low.getMillis()));
    // ..but be tracked per key / window
    assertThat(globalWindow.read(), is(Matchers.nullValue()));
    assertThat(fixedWindow.read(), is(Matchers.nullValue()));
    globalWindow.add(middle);
    fixedWindow.add(high);
    assertThat(globalWindow.read(), is(middle));
    assertThat(fixedWindow.read(), is(high));
    // Old key should give previous results
    keyedStateBackend.setCurrentKey(firstKey);
    assertThat(globalWindow.read(), is(low));
    assertThat(fixedWindow.read(), is(middle));

    // Discard watermark view and recover it
    stateInternals =
        new FlinkStateInternals<>(
            keyedStateBackend,
            StringUtf8Coder.of(),
            IntervalWindow.getCoder(),
            new SerializablePipelineOptions(FlinkPipelineOptions.defaults()));
    globalWindow = stateInternals.state(StateNamespaces.global(), stateTag);
    fixedWindow =
        stateInternals.state(
            StateNamespaces.window(
                IntervalWindow.getCoder(), new IntervalWindow(new Instant(0), new Instant(10))),
            stateTag);

    // Watermark hold across all keys should be unchanged
    assertThat(stateInternals.minWatermarkHoldMs(), is(low.getMillis()));

    // Check the holds for the second key and clear them
    keyedStateBackend.setCurrentKey(secondKey);
    assertThat(globalWindow.read(), is(middle));
    assertThat(fixedWindow.read(), is(high));
    globalWindow.clear();
    fixedWindow.clear();

    // Check the holds for the first key and clear them
    keyedStateBackend.setCurrentKey(firstKey);
    assertThat(globalWindow.read(), is(low));
    assertThat(fixedWindow.read(), is(middle));

    fixedWindow.clear();
    assertThat(stateInternals.minWatermarkHoldMs(), is(low.getMillis()));

    globalWindow.clear();
    assertThat(stateInternals.minWatermarkHoldMs(), is(noHold.getMillis()));
  }

  @Test
  public void testGlobalWindowWatermarkHoldClear() throws Exception {
    KeyedStateBackend<FlinkKey> keyedStateBackend = createStateBackend();
    FlinkStateInternals<String> stateInternals =
        new FlinkStateInternals<>(
            keyedStateBackend,
            StringUtf8Coder.of(),
            IntervalWindow.getCoder(),
            new SerializablePipelineOptions(FlinkPipelineOptions.defaults()));
    StateTag<WatermarkHoldState> stateTag =
        StateTags.watermarkStateInternal("hold", TimestampCombiner.EARLIEST);
    Instant now = Instant.now();
    WatermarkHoldState state = stateInternals.state(StateNamespaces.global(), stateTag);
    state.add(now);
    stateInternals.clearGlobalState();
    assertThat(state.read(), is((Instant) null));
  }

  @Test
  public void testMapStateStructuralKeys() throws Exception {
    FlinkStateInternals<String> stateInternals = newStateInternals(createStateBackend());

    StateTag<MapState<byte[], Integer>> tag =
        StateTags.map("byte-map", ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapState = stateInternals.state(StateNamespaces.global(), tag);

    mapState.put(new byte[] {1, 2}, 10);
    mapState.put(new byte[] {3}, 30);
    // Overwrite with a distinct byte[] instance having identical contents.
    mapState.put(new byte[] {1, 2}, 20);
    assertThat(mapState.putIfAbsent(new byte[] {1, 2}, 99).read(), is(20));
    assertThat(mapState.computeIfAbsent(new byte[] {4}, k -> 40).read(), is(nullValue()));

    assertThat(mapState.get(new byte[] {1, 2}).read(), is(20));
    assertThat(mapState.get(new byte[] {3}).read(), is(30));
    assertThat(mapState.get(new byte[] {4}).read(), is(40));
    assertThat(mapState.get(new byte[] {9}).read(), is(nullValue()));

    List<KV<String, Integer>> entries = new ArrayList<>();
    for (Map.Entry<byte[], Integer> entry : mapState.entries().read()) {
      entries.add(KV.of(Arrays.toString(entry.getKey()), entry.getValue()));
    }
    assertThat(
        entries, containsInAnyOrder(KV.of("[1, 2]", 20), KV.of("[3]", 30), KV.of("[4]", 40)));

    List<String> keys = new ArrayList<>();
    for (byte[] key : mapState.keys().read()) {
      keys.add(Arrays.toString(key));
    }
    assertThat(keys, containsInAnyOrder("[1, 2]", "[3]", "[4]"));

    mapState.remove(new byte[] {1, 2});
    assertThat(mapState.get(new byte[] {1, 2}).read(), is(nullValue()));
    mapState.remove(new byte[] {3});
    mapState.remove(new byte[] {4});
    assertThat(mapState.isEmpty().read(), is(true));
    assertThat(mapState.entries().read(), emptyIterable());
  }

  @Test
  public void testMapStateNullableKeys() throws Exception {
    FlinkStateInternals<String> stateInternals = newStateInternals(createStateBackend());

    StateTag<MapState<String, Integer>> tag =
        StateTags.map("nullable-map", NullableCoder.of(StringUtf8Coder.of()), VarIntCoder.of());
    MapState<String, Integer> mapState = stateInternals.state(StateNamespaces.global(), tag);

    mapState.put(null, 1);
    mapState.put("a", 2);
    mapState.put(null, 3);

    assertThat(mapState.get(null).read(), is(3));
    assertThat(mapState.get("a").read(), is(2));
    assertThat(mapState.keys().read(), containsInAnyOrder(null, "a"));

    mapState.remove(null);
    assertThat(mapState.get(null).read(), is(nullValue()));
    assertThat(mapState.keys().read(), containsInAnyOrder("a"));
  }

  @Test
  public void testSetStateStructuralValues() throws Exception {
    FlinkStateInternals<String> stateInternals = newStateInternals(createStateBackend());

    StateTag<SetState<byte[]>> tag = StateTags.set("byte-set", ByteArrayCoder.of());
    SetState<byte[]> setState = stateInternals.state(StateNamespaces.global(), tag);

    assertThat(setState.isEmpty().read(), is(true));
    setState.add(new byte[] {1, 2});
    setState.add(new byte[] {1, 2});
    assertThat(setState.addIfAbsent(new byte[] {1, 2}).read(), is(false));
    assertThat(setState.addIfAbsent(new byte[] {3}).read(), is(true));

    assertThat(setState.contains(new byte[] {1, 2}).read(), is(true));
    assertThat(setState.contains(new byte[] {3}).read(), is(true));
    assertThat(setState.contains(new byte[] {9}).read(), is(false));

    List<String> elements = new ArrayList<>();
    for (byte[] elem : setState.read()) {
      elements.add(Arrays.toString(elem));
    }
    assertThat(elements, containsInAnyOrder("[1, 2]", "[3]"));

    setState.remove(new byte[] {1, 2});
    assertThat(setState.contains(new byte[] {1, 2}).read(), is(false));
    setState.remove(new byte[] {3});
    assertThat(setState.isEmpty().read(), is(true));
  }

  /** Avro-encoded POJO without custom equals/hashCode, a common @DefaultCoder(AvroCoder) case. */
  public static class AvroEvent {
    public String id;
    public int count;

    public AvroEvent() {}

    public AvroEvent(String id, int count) {
      this.id = id;
      this.count = count;
    }
  }

  @Test
  public void testSetStateAvroReflectPojo() throws Exception {
    FlinkStateInternals<String> stateInternals = newStateInternals(createStateBackend());

    StateTag<SetState<AvroEvent>> pojoTag =
        StateTags.set("avro-pojo-set", AvroCoder.of(AvroEvent.class));
    SetState<AvroEvent> pojoSet = stateInternals.state(StateNamespaces.global(), pojoTag);

    pojoSet.add(new AvroEvent("evt-1", 5));
    assertThat(pojoSet.contains(new AvroEvent("evt-1", 5)).read(), is(true));
    assertThat(pojoSet.addIfAbsent(new AvroEvent("evt-1", 5)).read(), is(false));
    assertThat(pojoSet.contains(new AvroEvent("evt-1", 6)).read(), is(false));
    pojoSet.remove(new AvroEvent("evt-1", 5));
    assertThat(pojoSet.isEmpty().read(), is(true));
  }

  private static FlinkStateInternals<String> newStateInternals(
      KeyedStateBackend<FlinkKey> keyedStateBackend) throws Exception {
    return new FlinkStateInternals<>(
        keyedStateBackend,
        StringUtf8Coder.of(),
        IntervalWindow.getCoder(),
        new SerializablePipelineOptions(FlinkPipelineOptions.defaults()));
  }

  public static KeyedStateBackend<FlinkKey> createStateBackend() throws Exception {
    AbstractKeyedStateBackend<FlinkKey> keyedStateBackend =
        MemoryStateBackendWrapper.createKeyedStateBackend(
            new DummyEnvironment("test", 1, 0),
            new JobID(),
            "test_op",
            new ValueTypeInfo<>(FlinkKey.class).createSerializer(new SerializerConfigImpl()),
            2,
            new KeyGroupRange(0, 1),
            new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
            TtlTimeProvider.DEFAULT,
            null,
            Collections.emptyList(),
            new CloseableRegistry());
    changeKey(keyedStateBackend);
    return keyedStateBackend;
  }

  private static void changeKey(KeyedStateBackend<FlinkKey> keyedStateBackend)
      throws CoderException {
    keyedStateBackend.setCurrentKey(
        FlinkKey.of(
            ByteBuffer.wrap(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), UUID.randomUUID().toString()))));
  }
}
