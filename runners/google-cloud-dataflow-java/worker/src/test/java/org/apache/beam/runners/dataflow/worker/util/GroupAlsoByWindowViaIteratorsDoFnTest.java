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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link BatchGroupAlsoByWindowViaIteratorsFn}.
 *
 * <p>Note the absence of tests for sessions, as merging window functions are not supported.
 */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowViaIteratorsDoFnTest {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private class GABWViaIteratorsDoFnFactory<K, InputT>
      implements GroupAlsoByWindowDoFnFactory<K, InputT, Iterable<InputT>> {
    @Override
    public <W extends BoundedWindow>
        BatchGroupAlsoByWindowFn<K, InputT, Iterable<InputT>> forStrategy(
            WindowingStrategy<?, W> windowingStrategy,
            StateInternalsFactory<K> stateInternalsFactory) {
      return new BatchGroupAlsoByWindowViaIteratorsFn<K, InputT, W>(windowingStrategy);
    }
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowProperties.emptyInputEmptyOutput(new GABWViaIteratorsDoFnFactory<>());
  }

  @Test
  public void testGroupsElementsIntoFixedWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsElementsIntoSlidingWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoSlidingWindowsWithMinTimestamp(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsIntoOverlappingNonmergingWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsIntoOverlappingNonmergingWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsElementsIntoFixedWindowsWithEndOfWindowTimestamp() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindowsWithEndOfWindowTimestamp(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testLatestTimestampNotSupported() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TimestampCombiner");
    thrown.expectMessage("not support");

    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindowsWithLatestTimestamp(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testMergingNotSupported() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("merging");
    thrown.expectMessage("not support");

    GroupAlsoByWindowProperties.groupsElementsInMergedSessions(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }
}
