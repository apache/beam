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
package org.apache.beam.runners.dataflow.worker.util;

import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.dataflow.worker.util.GroupAlsoByWindowProperties.GroupAlsoByWindowDoFnFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BatchGroupAlsoByWindowViaOutputBufferFn} that use a combining {@link
 * SystemReduceFn}.
 */
@RunWith(JUnit4.class)
public class CombiningGroupAlsoByWindowsViaOutputBufferDoFnTest {

  private class CombiningGABWViaOutputBufferDoFnFactory<K, InputT, AccumT, OutputT>
      implements GroupAlsoByWindowDoFnFactory<K, InputT, OutputT> {

    private final Coder<K> keyCoder;
    private final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn;

    public CombiningGABWViaOutputBufferDoFnFactory(
        Coder<K> keyCoder, AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
      this.keyCoder = keyCoder;
      this.combineFn = combineFn;
    }

    @Override
    public <W extends BoundedWindow> BatchGroupAlsoByWindowFn<K, InputT, OutputT> forStrategy(
        WindowingStrategy<?, W> windowingStrategy, StateInternalsFactory<K> stateInternalsFactory) {
      return new BatchGroupAlsoByWindowViaOutputBufferFn<>(
          windowingStrategy,
          stateInternalsFactory,
          SystemReduceFn.<K, InputT, AccumT, OutputT, W>combining(keyCoder, combineFn));
    }
  }

  @Test
  public void testCombinesElementsInSlidingWindows() throws Exception {
    CombineFn<Long, ?, Long> combineFn = Sum.ofLongs();
    AppliedCombineFn<String, Long, ?, Long> appliedFn =
        AppliedCombineFn.withInputCoder(
            combineFn,
            CoderRegistry.createDefault(),
            KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

    GroupAlsoByWindowProperties.combinesElementsInSlidingWindows(
        new CombiningGABWViaOutputBufferDoFnFactory<>(StringUtf8Coder.of(), appliedFn), combineFn);
  }

  @Test
  public void testCombinesIntoSessions() throws Exception {
    CombineFn<Long, ?, Long> combineFn = Sum.ofLongs();
    AppliedCombineFn<String, Long, ?, Long> appliedFn =
        AppliedCombineFn.withInputCoder(
            combineFn,
            CoderRegistry.createDefault(),
            KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

    GroupAlsoByWindowProperties.combinesElementsPerSession(
        new CombiningGABWViaOutputBufferDoFnFactory<>(StringUtf8Coder.of(), appliedFn), combineFn);
  }

  @Test
  public void testCombinesIntoSessionsWithEndOfWindowTimestamp() throws Exception {
    CombineFn<Long, ?, Long> combineFn = Sum.ofLongs();
    AppliedCombineFn<String, Long, ?, Long> appliedFn =
        AppliedCombineFn.withInputCoder(
            combineFn,
            CoderRegistry.createDefault(),
            KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

    GroupAlsoByWindowProperties.combinesElementsPerSessionWithEndOfWindowTimestamp(
        new CombiningGABWViaOutputBufferDoFnFactory<>(StringUtf8Coder.of(), appliedFn), combineFn);
  }
}
