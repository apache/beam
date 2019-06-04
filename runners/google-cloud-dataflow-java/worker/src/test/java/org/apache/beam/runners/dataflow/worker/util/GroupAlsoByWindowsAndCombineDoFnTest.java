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
import org.apache.beam.runners.dataflow.worker.util.GroupAlsoByWindowProperties.GroupAlsoByWindowDoFnFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BatchGroupAlsoByWindowAndCombineFn}. */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowsAndCombineDoFnTest {

  private class GABWAndCombineDoFnFactory<K, InputT, AccumT, OutputT>
      implements GroupAlsoByWindowDoFnFactory<K, InputT, OutputT> {

    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    public GABWAndCombineDoFnFactory(CombineFn<InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public <W extends BoundedWindow> BatchGroupAlsoByWindowFn<K, InputT, OutputT> forStrategy(
        WindowingStrategy<?, W> windowingStrategy, StateInternalsFactory<K> stateInternals) {

      return new BatchGroupAlsoByWindowAndCombineFn<>(windowingStrategy, combineFn);
    }
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowProperties.emptyInputEmptyOutput(
        new GABWAndCombineDoFnFactory<>(Sum.ofLongs()));
  }

  @Test
  public void testCombinesElementsInSlidingWindows() throws Exception {
    CombineFn<Long, ?, Long> combineFn = Sum.ofLongs();

    GroupAlsoByWindowProperties.combinesElementsInSlidingWindows(
        new GABWAndCombineDoFnFactory<>(combineFn), combineFn);
  }

  @Test
  public void testCombinesIntoSessions() throws Exception {
    CombineFn<Long, ?, Long> combineFn = Sum.ofLongs();

    GroupAlsoByWindowProperties.combinesElementsPerSession(
        new GABWAndCombineDoFnFactory<>(combineFn), combineFn);
  }

  @Test
  public void testCombinesIntoSessionsWithEndOfWindowTimestamp() throws Exception {
    CombineFn<Long, ?, Long> combineFn = Sum.ofLongs();

    GroupAlsoByWindowProperties.combinesElementsPerSessionWithEndOfWindowTimestamp(
        new GABWAndCombineDoFnFactory<>(combineFn), combineFn);
  }
}
