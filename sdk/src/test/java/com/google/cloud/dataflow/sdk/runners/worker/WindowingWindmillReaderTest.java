/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import static com.google.cloud.dataflow.sdk.util.common.worker.Reader.ReaderIterator;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerOrElement;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link WindowingWindmillReader}. */
@RunWith(JUnit4.class)
public class WindowingWindmillReaderTest {
  private static final String STATE_FAMILY = "state";
  private static final String KEY = "key";
  private static final ByteString SERIALIZED_KEY = ByteString.copyFromUtf8("key");
  private static final StateNamespace STATE_NAMESPACE =
      StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);

  @Mock
  StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }


  // Make sure that event time timers are processed before processing time ones.
  @Test
  public void testTimerOrdering() throws Exception {
    when(mockContext.getWork()).thenReturn(
        Windmill.WorkItem.newBuilder()
        .setKey(SERIALIZED_KEY)
        .setWorkToken(17)
        .setTimers(Windmill.TimerBundle.newBuilder()
            .addTimers(makeSerializedTimer("Processing-1", Windmill.Timer.Type.REALTIME))
            .addTimers(makeSerializedTimer("Event-1", Windmill.Timer.Type.WATERMARK))
            .addTimers(makeSerializedTimer("Processing-2", Windmill.Timer.Type.REALTIME))
            .addTimers(makeSerializedTimer("Event-2", Windmill.Timer.Type.WATERMARK))
            .build())
        .build());
    when(mockContext.getSerializedKey()).thenReturn(SERIALIZED_KEY);

    Coder<WindowedValue<TimerOrElement<KV<String, String>>>> coder =
        WindowedValue.getFullCoder(
            TimerOrElement.TimerOrElementCoder.of(
                KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())),
            GlobalWindow.Coder.INSTANCE);

    ReaderIterator<WindowedValue<TimerOrElement<KV<String, String>>>> iterator =
        WindowingWindmillReader.<KV<String, String>>create(null, null, coder, mockContext)
            .iterator();
    List<WindowedValue<TimerOrElement<KV<String, String>>>> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    assertThat(result, contains(
        makeTimer(TimeDomain.EVENT_TIME),
        makeTimer(TimeDomain.EVENT_TIME),
        makeTimer(TimeDomain.PROCESSING_TIME),
        makeTimer(TimeDomain.PROCESSING_TIME)));
  }

  private static Windmill.Timer makeSerializedTimer(String name, Windmill.Timer.Type type) {
    return Windmill.Timer.newBuilder()
        .setTag(ByteString.copyFromUtf8(STATE_NAMESPACE.stringKey() + "+" + name))
        .setTimestamp(0)
        .setType(type)
        .setStateFamily(STATE_FAMILY)
        .build();
  }

  private static WindowedValue<TimerOrElement<KV<String, String>>> makeTimer(TimeDomain domain) {
    return WindowedValue.<TimerOrElement<KV<String, String>>>of(
        TimerOrElement.<KV<String, String>>timer(
            KEY, TimerData.of(STATE_NAMESPACE, new Instant(0), domain)),
        new Instant(0), Arrays.<BoundedWindow>asList(), PaneInfo.NO_FIRING);
  }
}
