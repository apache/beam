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
import static org.hamcrest.core.Is.is;

import java.nio.ByteBuffer;
import java.util.Collections;
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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
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

  public static KeyedStateBackend<FlinkKey> createStateBackend() throws Exception {
    AbstractKeyedStateBackend<FlinkKey> keyedStateBackend =
        MemoryStateBackendWrapper.createKeyedStateBackend(
            new DummyEnvironment("test", 1, 0),
            new JobID(),
            "test_op",
            new ValueTypeInfo<>(FlinkKey.class).createSerializer(new ExecutionConfig()),
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
        FlinkKey.of(ByteBuffer.wrap(
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), UUID.randomUUID().toString()))));
  }
}
