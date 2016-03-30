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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/** Test case for {@link WindowedValue}. */
@RunWith(JUnit4.class)
public class WindowedValueTest {
  @Test
  public void testWindowedValueCoder() throws CoderException {
    Instant timestamp = new Instant(1234);
    WindowedValue<String> value = WindowedValue.of(
        "abc",
        new Instant(1234),
        Arrays.asList(new IntervalWindow(timestamp, timestamp.plus(1000)),
                      new IntervalWindow(timestamp.plus(1000), timestamp.plus(2000))),
        PaneInfo.NO_FIRING);

    Coder<WindowedValue<String>> windowedValueCoder =
        WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    byte[] encodedValue = CoderUtils.encodeToByteArray(windowedValueCoder, value);
    WindowedValue<String> decodedValue =
        CoderUtils.decodeFromByteArray(windowedValueCoder, encodedValue);

    Assert.assertEquals(value.getValue(), decodedValue.getValue());
    Assert.assertEquals(value.getTimestamp(), decodedValue.getTimestamp());
    Assert.assertArrayEquals(value.getWindows().toArray(), decodedValue.getWindows().toArray());
  }
}
