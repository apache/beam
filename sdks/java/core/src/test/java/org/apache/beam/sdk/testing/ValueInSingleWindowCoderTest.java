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
package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueInSingleWindow.Coder}. */
@RunWith(JUnit4.class)
public class ValueInSingleWindowCoderTest {
  @Test
  public void testDecodeEncodeEqual() throws Exception {
    Instant now = Instant.now();
    ValueInSingleWindow<String> value =
        ValueInSingleWindow.of(
            "foo",
            now,
            new IntervalWindow(now, now.plus(Duration.standardSeconds(10))),
            PaneInfo.NO_FIRING);

    CoderProperties.coderDecodeEncodeEqual(
        ValueInSingleWindow.Coder.of(StringUtf8Coder.of(), IntervalWindow.getCoder()), value);
  }

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(
        ValueInSingleWindow.Coder.of(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        ValueInSingleWindow.Coder.of(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }
}
