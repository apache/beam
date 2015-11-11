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

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowsProperties.GroupAlsoByWindowsDoFnFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GroupAlsoByWindowsAndCombineDoFn}. */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowsAndCombineDoFnTest {

  private class GABWAndCombineDoFnFactory<K, InputT, AccumT, OutputT>
  implements GroupAlsoByWindowsDoFnFactory<K, InputT, OutputT> {

    private final KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn;

    public GABWAndCombineDoFnFactory(
        KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn) {
      this.keyedCombineFn = keyedCombineFn;
    }

    @Override
    public <W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, InputT, OutputT, W>
        forStrategy(WindowingStrategy<?, W> windowingStrategy) {

      @SuppressWarnings("unchecked")
      WindowFn<Object, W> windowFn = (WindowFn<Object, W>) windowingStrategy.getWindowFn();

      return new GroupAlsoByWindowsAndCombineDoFn<K, InputT, AccumT, OutputT, W>(
          windowFn,
          keyedCombineFn);
    }
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowsProperties.emptyInputEmptyOutput(
        new GABWAndCombineDoFnFactory<>(new Sum.SumLongFn().asKeyedFn()));
  }

  @Test
  public void testCombinesElementsInSlidingWindows() throws Exception {
    CombineFn<Long, ?, Long> combineFn = new Sum.SumLongFn();

    GroupAlsoByWindowsProperties.combinesElementsInSlidingWindows(
        new GABWAndCombineDoFnFactory<>(combineFn.<String>asKeyedFn()),
        combineFn);
  }

  @Test
  public void testCombinesIntoSessions() throws Exception {
    CombineFn<Long, ?, Long> combineFn = new Sum.SumLongFn();

    GroupAlsoByWindowsProperties.combinesElementsPerSession(
        new GABWAndCombineDoFnFactory<>(combineFn.<String>asKeyedFn()),
        combineFn);
  }

}
