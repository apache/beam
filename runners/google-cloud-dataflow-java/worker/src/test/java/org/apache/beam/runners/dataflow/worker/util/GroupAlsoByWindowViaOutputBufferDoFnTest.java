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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BatchGroupAlsoByWindowViaOutputBufferFn}. */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowViaOutputBufferDoFnTest {

  private class BufferingGABWViaOutputBufferDoFnFactory<K, InputT>
      implements GroupAlsoByWindowDoFnFactory<K, InputT, Iterable<InputT>> {

    private final Coder<InputT> inputCoder;

    public BufferingGABWViaOutputBufferDoFnFactory(Coder<InputT> inputCoder) {
      this.inputCoder = inputCoder;
    }

    @Override
    public <W extends BoundedWindow>
        BatchGroupAlsoByWindowFn<K, InputT, Iterable<InputT>> forStrategy(
            WindowingStrategy<?, W> windowingStrategy,
            StateInternalsFactory<K> stateInternalsFactory) {
      return new BatchGroupAlsoByWindowViaOutputBufferFn<K, InputT, Iterable<InputT>, W>(
          windowingStrategy,
          stateInternalsFactory,
          SystemReduceFn.<K, InputT, W>buffering(inputCoder));
    }
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowProperties.emptyInputEmptyOutput(
        new BufferingGABWViaOutputBufferDoFnFactory<>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsElementsIntoFixedWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindows(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsElementsIntoSlidingWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoSlidingWindowsWithMinTimestamp(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsIntoOverlappingNonmergingWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsIntoOverlappingNonmergingWindows(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsIntoSessions() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsInMergedSessions(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsElementsIntoFixedWindowsWithEndOfWindowTimestamp() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindowsWithEndOfWindowTimestamp(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsElementsIntoFixedWindowsWithLatestTimestamp() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindowsWithLatestTimestamp(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsElementsIntoSessionsWithEndOfWindowTimestamp() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsInMergedSessionsWithEndOfWindowTimestamp(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }

  @Test
  public void testGroupsElementsIntoSessionsWithLatestTimestamp() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsInMergedSessionsWithLatestTimestamp(
        new BufferingGABWViaOutputBufferDoFnFactory<String, String>(StringUtf8Coder.of()));
  }
}
