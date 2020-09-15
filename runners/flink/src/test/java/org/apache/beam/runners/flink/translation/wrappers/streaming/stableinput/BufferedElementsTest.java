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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link BufferedElements}. */
public class BufferedElementsTest {

  @Test
  public void testCoder() throws IOException {

    StringUtf8Coder elementCoder = StringUtf8Coder.of();
    // Generics fail to see here that this is Coder<BoundedWindow>
    org.apache.beam.sdk.coders.Coder windowCoder = GlobalWindow.Coder.INSTANCE;
    WindowedValue.WindowedValueCoder windowedValueCoder =
        WindowedValue.FullWindowedValueCoder.of(elementCoder, windowCoder);
    KV<String, Integer> key = KV.of("one", 1);
    BufferedElements.Coder coder = new BufferedElements.Coder(windowedValueCoder, windowCoder, key);

    BufferedElement element =
        new BufferedElements.Element(
            WindowedValue.of("test", new Instant(2), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
    BufferedElement timerElement =
        new BufferedElements.Timer(
            "timerId",
            "timerId",
            key,
            GlobalWindow.INSTANCE,
            new Instant(1),
            new Instant(1),
            TimeDomain.EVENT_TIME);

    testRoundTrip(ImmutableList.of(element), coder);
    testRoundTrip(ImmutableList.of(timerElement), coder);
    testRoundTrip(ImmutableList.of(element, timerElement), coder);
    testRoundTrip(ImmutableList.of(element, timerElement, element), coder);
    testRoundTrip(ImmutableList.of(element, element, element, timerElement, timerElement), coder);
  }

  private static void testRoundTrip(
      List<BufferedElement> bufferedElements, BufferedElements.Coder coder) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (BufferedElement bufferedElement : bufferedElements) {
      coder.encode(bufferedElement, baos);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    for (BufferedElement bufferedElement : bufferedElements) {
      assertThat(coder.decode(bais), Matchers.is(bufferedElement));
    }
  }
}
