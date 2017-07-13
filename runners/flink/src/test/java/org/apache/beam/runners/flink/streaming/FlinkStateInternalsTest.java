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

import java.nio.ByteBuffer;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsTest;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlinkStateInternals}. This is based on {@link StateInternalsTest}.
 */
@RunWith(JUnit4.class)
public class FlinkStateInternalsTest extends StateInternalsTest {

  @Override
  protected StateInternals createStateInternals() {
    MemoryStateBackend backend = new MemoryStateBackend();
    try {
      AbstractKeyedStateBackend<ByteBuffer> keyedStateBackend = backend.createKeyedStateBackend(
          new DummyEnvironment("test", 1, 0),
          new JobID(),
          "test_op",
          new GenericTypeInfo<>(ByteBuffer.class).createSerializer(new ExecutionConfig()),
          1,
          new KeyGroupRange(0, 0),
          new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));

      keyedStateBackend.setCurrentKey(
          ByteBuffer.wrap(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "Hello")));

      return new FlinkStateInternals<>(keyedStateBackend, StringUtf8Coder.of());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
