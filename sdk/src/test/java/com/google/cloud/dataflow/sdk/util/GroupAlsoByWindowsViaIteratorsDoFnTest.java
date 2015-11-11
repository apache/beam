/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowsProperties.GroupAlsoByWindowsDoFnFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link GroupAlsoByWindowsViaIteratorsDoFn}.
 *
 * <p>Note the absence of tests for sessions, as merging window functions are not supported.
 */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowsViaIteratorsDoFnTest {

  @Rule
  public final transient ExpectedException thrown = ExpectedException.none();

  private class GABWViaIteratorsDoFnFactory<K, InputT>
  implements GroupAlsoByWindowsDoFnFactory<K, InputT, Iterable<InputT>> {
    @Override
    public <W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, InputT, Iterable<InputT>, W>
        forStrategy(WindowingStrategy<?, W> windowingStrategy) {
      return new GroupAlsoByWindowsViaIteratorsDoFn<K, InputT, W>(windowingStrategy);
    }
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowsProperties.emptyInputEmptyOutput(
        new GABWViaIteratorsDoFnFactory<>());
  }

  @Test
  public void testGroupsElementsIntoFixedWindows() throws Exception {
    GroupAlsoByWindowsProperties.groupsElementsIntoFixedWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsElementsIntoSlidingWindows() throws Exception {
    GroupAlsoByWindowsProperties.groupsElementsIntoSlidingWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsIntoOverlappingNonmergingWindows() throws Exception {
    GroupAlsoByWindowsProperties.groupsIntoOverlappingNonmergingWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testMergingNotSupported() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("merging");
    thrown.expectMessage("not support");

    GroupAlsoByWindowsProperties.groupsElementsInMergedSessions(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }
}
