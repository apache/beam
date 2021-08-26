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
package org.apache.beam.runners.core;

import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KeyedWorkItems}. */
@RunWith(JUnit4.class)
public class KeyedWorkItemCoderTest {
  @Test
  public void testCoderProperties() throws Exception {
    CoderProperties.coderSerializable(
        KeyedWorkItemCoder.of(StringUtf8Coder.of(), VarIntCoder.of(), GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testEncodeDecodeEqual() throws Exception {
    Iterable<TimerData> timers =
        ImmutableList.of(
            TimerData.of(
                StateNamespaces.global(),
                new Instant(500L),
                new Instant(500L),
                TimeDomain.EVENT_TIME));
    Iterable<WindowedValue<Integer>> elements =
        ImmutableList.of(
            WindowedValue.valueInGlobalWindow(1),
            WindowedValue.valueInGlobalWindow(4),
            WindowedValue.valueInGlobalWindow(8));

    KeyedWorkItemCoder<String, Integer> coder =
        KeyedWorkItemCoder.of(StringUtf8Coder.of(), VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);

    CoderProperties.coderDecodeEncodeEqual(coder, KeyedWorkItems.workItem("foo", timers, elements));
    CoderProperties.coderDecodeEncodeEqual(coder, KeyedWorkItems.elementsWorkItem("foo", elements));
    CoderProperties.coderDecodeEncodeEqual(coder, KeyedWorkItems.timersWorkItem("foo", timers));
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(
        KeyedWorkItemCoder.of(
            GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }
}
