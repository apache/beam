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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the static factory methods in the abstract
 * class {@link GroupAlsoByWindowsDoFn}.
 */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowsDoFnTest {

  @Test
  public void testCreateNoncombiningNonmerging() throws Exception {
    Coder<Long> inputCoder = VarLongCoder.of();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    assertThat(
        GroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, inputCoder),
        instanceOf(GroupAlsoByWindowsViaIteratorsDoFn.class));
  }

  @Test
  public void testCreateNoncombiningMerging() throws Exception {
    Coder<Long> inputCoder = VarLongCoder.of();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    assertThat(
        GroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, inputCoder),
        instanceOf(GroupAlsoByWindowsViaOutputBufferDoFn.class));
  }

  @Test
  public void testCreateNoncombiningWithTrigger() throws Exception {
    Coder<Long> inputCoder = VarLongCoder.of();
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)))
        .withTrigger(AfterPane.elementCountAtLeast(1));

    assertThat(
        GroupAlsoByWindowsDoFn.createForIterable(windowingStrategy, inputCoder),
        instanceOf(GroupAlsoByWindowsViaOutputBufferDoFn.class));
  }

  @Test
  public void testCreateCombiningNonmerging() throws Exception {
    Coder<String> keyCoder = StringUtf8Coder.of();
    AppliedCombineFn<String, Long, ?, Long> appliedFn = AppliedCombineFn.withInputCoder(
        new Sum.SumLongFn().<String>asKeyedFn(),
        new CoderRegistry(),
        KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));

    assertThat(
        GroupAlsoByWindowsDoFn.create(windowingStrategy,
            appliedFn,
            keyCoder),
        instanceOf(GroupAlsoByWindowsAndCombineDoFn.class));
  }

  @Test
  public void testCreateCombiningMerging() throws Exception {
    Coder<String> keyCoder = StringUtf8Coder.of();
    AppliedCombineFn<String, Long, ?, Long> appliedFn = AppliedCombineFn.withInputCoder(
        new Sum.SumLongFn().<String>asKeyedFn(),
        new CoderRegistry(),
        KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)));

    assertThat(
        GroupAlsoByWindowsDoFn.create(windowingStrategy,
            appliedFn,
            keyCoder),
        instanceOf(GroupAlsoByWindowsAndCombineDoFn.class));
  }

  @Test
  public void testCreateCombiningWithTrigger() throws Exception {
    Coder<String> keyCoder = StringUtf8Coder.of();
    AppliedCombineFn<String, Long, ?, Long> appliedFn = AppliedCombineFn.withInputCoder(
        new Sum.SumLongFn().<String>asKeyedFn(),
        new CoderRegistry(),
        KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
    WindowingStrategy<?, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(10)))
        .withTrigger(AfterPane.elementCountAtLeast(1));

    assertThat(
        GroupAlsoByWindowsDoFn.create(windowingStrategy,
            appliedFn,
            keyCoder),
        instanceOf(GroupAlsoByWindowsViaOutputBufferDoFn.class));
  }
}
