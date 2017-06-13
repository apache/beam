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

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsTest;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkBroadcastStateInternals;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlinkBroadcastStateInternals}. This is based on the tests for
 * {@code StateInternalsTest}.
 *
 * <p>Just test value, bag and combining.
 */
@RunWith(JUnit4.class)
public class FlinkBroadcastStateInternalsTest extends StateInternalsTest {

  @Override
  protected StateInternals createStateInternals() {
    MemoryStateBackend backend = new MemoryStateBackend();
    try {
      OperatorStateBackend operatorStateBackend =
          backend.createOperatorStateBackend(new DummyEnvironment("test", 1, 0), "");
      return new FlinkBroadcastStateInternals<>(1, operatorStateBackend);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  ///////////////////////// Unsupported tests \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  @Override
  public void testSet() {}

  @Override
  public void testSetIsEmpty() {}

  @Override
  public void testMergeSetIntoSource() {}

  @Override
  public void testMergeSetIntoNewNamespace() {}

  @Override
  public void testMap() {}

  @Override
  public void testWatermarkEarliestState() {}

  @Override
  public void testWatermarkLatestState() {}

  @Override
  public void testWatermarkEndOfWindowState() {}

  @Override
  public void testWatermarkStateIsEmpty() {}

  @Override
  public void testMergeEarliestWatermarkIntoSource() {}

  @Override
  public void testMergeLatestWatermarkIntoSource() {}

  @Override
  public void testSetReadable() {}

  @Override
  public void testMapReadable() {}

}
