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

import java.util.Collections;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsTest;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkBroadcastStateInternals;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlinkBroadcastStateInternals}. This is based on the tests for {@code
 * StateInternalsTest}.
 *
 * <p>Just test value, bag and combining.
 */
@RunWith(JUnit4.class)
public class FlinkBroadcastStateInternalsTest extends StateInternalsTest {

  @Override
  protected StateInternals createStateInternals() {
    try {
      OperatorStateBackend operatorStateBackend =
          MemoryStateBackendWrapper.createOperatorStateBackend(
              new DummyEnvironment("test", 1, 0), "", Collections.emptyList(), null);
      return new FlinkBroadcastStateInternals<>(
          1,
          operatorStateBackend,
          new SerializablePipelineOptions(FlinkPipelineOptions.defaults()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @Ignore
  public void testSet() {}

  @Override
  @Ignore
  public void testSetIsEmpty() {}

  @Override
  @Ignore
  public void testMergeSetIntoSource() {}

  @Override
  @Ignore
  public void testMergeSetIntoNewNamespace() {}

  @Override
  @Ignore
  public void testMap() {}

  @Override
  @Ignore
  public void testWatermarkEarliestState() {}

  @Override
  @Ignore
  public void testWatermarkLatestState() {}

  @Override
  @Ignore
  public void testWatermarkEndOfWindowState() {}

  @Override
  @Ignore
  public void testWatermarkStateIsEmpty() {}

  @Override
  @Ignore
  public void testSetReadable() {}

  @Override
  @Ignore
  public void testMapReadable() {}
}
