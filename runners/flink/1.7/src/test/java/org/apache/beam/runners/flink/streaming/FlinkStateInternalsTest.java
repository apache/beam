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
import java.util.UUID;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsTest;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlinkStateInternals}. This is based on {@link StateInternalsTest}. */
@RunWith(JUnit4.class)
public class FlinkStateInternalsTest extends StateInternalsTest {

  @Override
  protected StateInternals createStateInternals() {
    try {
      KeyedStateBackend<ByteBuffer> keyedStateBackend = createStateBackend();
      return new FlinkStateInternals<>(keyedStateBackend, StringUtf8Coder.of());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testWatermarkHoldsPersistence() throws Exception {
    KeyedStateBackend<ByteBuffer> keyedStateBackend = createStateBackend();
    FlinkStateInternals stateInternals =
        new FlinkStateInternals<>(keyedStateBackend, StringUtf8Coder.of());

    StateTag<WatermarkHoldState> stateTag =
        StateTags.watermarkStateInternal("hold", TimestampCombiner.EARLIEST);
    WatermarkHoldState globalWindow = stateInternals.state(StateNamespaces.global(), stateTag);
    WatermarkHoldState fixedWindow =
        stateInternals.state(
            StateNamespaces.window(
                IntervalWindow.getCoder(), new IntervalWindow(new Instant(0), new Instant(10))),
            stateTag);

    Instant noHold = new Instant(Long.MAX_VALUE);
    assertThat(stateInternals.watermarkHold(), is(noHold));

    Instant high = new Instant(10);
    globalWindow.add(high);
    assertThat(stateInternals.watermarkHold(), is(high));

    Instant middle = new Instant(5);
    fixedWindow.add(middle);
    assertThat(stateInternals.watermarkHold(), is(middle));

    Instant low = new Instant(1);
    globalWindow.add(low);
    assertThat(stateInternals.watermarkHold(), is(low));

    // Try to overwrite with later hold (should not succeed)
    globalWindow.add(high);
    assertThat(stateInternals.watermarkHold(), is(low));
    fixedWindow.add(high);
    assertThat(stateInternals.watermarkHold(), is(low));

    changeKey(keyedStateBackend);
    // Discard watermark view and recover it
    stateInternals = new FlinkStateInternals<>(keyedStateBackend, StringUtf8Coder.of());
    globalWindow = stateInternals.state(StateNamespaces.global(), stateTag);
    fixedWindow =
        stateInternals.state(
            StateNamespaces.window(
                IntervalWindow.getCoder(), new IntervalWindow(new Instant(0), new Instant(10))),
            stateTag);

    assertThat(stateInternals.watermarkHold(), is(low));

    fixedWindow.clear();
    assertThat(stateInternals.watermarkHold(), is(low));

    globalWindow.clear();
    assertThat(stateInternals.watermarkHold(), is(noHold));
  }

  public static KeyedStateBackend<ByteBuffer> createStateBackend() throws Exception {
    MemoryStateBackend backend = new MemoryStateBackend();

    AbstractKeyedStateBackend<ByteBuffer> keyedStateBackend =
        backend.createKeyedStateBackend(
            new DummyEnvironment("test", 1, 0),
            new JobID(),
            "test_op",
            new GenericTypeInfo<>(ByteBuffer.class).createSerializer(new ExecutionConfig()),
            2,
            new KeyGroupRange(0, 1),
            new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));

    changeKey(keyedStateBackend);

    return keyedStateBackend;
  }

  private static void changeKey(KeyedStateBackend<ByteBuffer> keyedStateBackend)
      throws CoderException {
    keyedStateBackend.setCurrentKey(
        ByteBuffer.wrap(
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), UUID.randomUUID().toString())));
  }
}
