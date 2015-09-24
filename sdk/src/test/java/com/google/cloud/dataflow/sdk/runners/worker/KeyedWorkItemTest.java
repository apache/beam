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
package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CollectionCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.protobuf.ByteString;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** Tests for {@link KeyedWorkItem}. */
@RunWith(JUnit4.class)
public class KeyedWorkItemTest {

  private static final String STATE_FAMILY = "state";
  private static final String KEY = "key";
  private static final ByteString SERIALIZED_KEY = ByteString.copyFromUtf8(KEY);

  private static final Coder<IntervalWindow> WINDOW_CODER = IntervalWindow.getCoder();
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final Coder<Collection<? extends BoundedWindow>> WINDOWS_CODER =
      (Coder) CollectionCoder.of(WINDOW_CODER);
  private static final Coder<String> VALUE_CODER = StringUtf8Coder.of();
  private static final IntervalWindow WINDOW_1 =
      new IntervalWindow(new Instant(0), new Instant(10));
  private static final StateNamespace STATE_NAMESPACE_1 =
      StateNamespaces.window(WINDOW_CODER, WINDOW_1);
  private static final IntervalWindow WINDOW_2 =
      new IntervalWindow(new Instant(10), new Instant(20));
  private static final StateNamespace STATE_NAMESPACE_2 =
      StateNamespaces.window(WINDOW_CODER, WINDOW_2);

  @Mock
  private StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testElementIteration() throws Exception {
    Windmill.WorkItem.Builder workItem = Windmill.WorkItem.newBuilder()
        .setKey(SERIALIZED_KEY)
        .setWorkToken(17);
    Windmill.InputMessageBundle.Builder chunk1 = workItem.addMessageBundlesBuilder();
    chunk1.setSourceComputationId("computation");
    addElement(chunk1, 5, "hello", WINDOW_1, paneInfo(0));
    addElement(chunk1, 7, "world", WINDOW_2, paneInfo(2));
    Windmill.InputMessageBundle.Builder chunk2 = workItem.addMessageBundlesBuilder();
    chunk2.setSourceComputationId("computation");
    addElement(chunk2, 6, "earth", WINDOW_1, paneInfo(1));

    KeyedWorkItem<String> keyedWorkItem =
        KeyedWorkItem.workItem(KEY, workItem.build(), WINDOW_CODER, WINDOWS_CODER, VALUE_CODER);

    assertThat(keyedWorkItem.elementsIterable(), Matchers.contains(
        WindowedValue.of("hello", new Instant(5), WINDOW_1, paneInfo(0)),
        WindowedValue.of("world", new Instant(7), WINDOW_2, paneInfo(2)),
        WindowedValue.of("earth", new Instant(6), WINDOW_1, paneInfo(1))));
  }

  private void addElement(
      Windmill.InputMessageBundle.Builder chunk, long timestamp, String value,
      IntervalWindow window, PaneInfo pane) throws IOException {
    ByteString encodedMetadata =
        WindmillSink.encodeMetadata(WINDOWS_CODER, Collections.singletonList(window), pane);
    chunk.addMessagesBuilder()
        .setTimestamp(TimeUnit.MILLISECONDS.toMicros(timestamp))
        .setData(ByteString.copyFromUtf8(value))
        .setMetadata(encodedMetadata);
  }

  private PaneInfo paneInfo(int index) {
    return PaneInfo.createPane(false, false, Timing.EARLY, index, -1);
  }

  /** Make sure that event time timers are processed before other timers. */
  @Test
  public void testTimerOrdering() throws Exception {
    Windmill.WorkItem workItem = Windmill.WorkItem.newBuilder()
        .setKey(SERIALIZED_KEY)
        .setWorkToken(17)
        .setTimers(Windmill.TimerBundle.newBuilder()
            .addTimers(makeSerializedTimer(STATE_NAMESPACE_1, 0, Windmill.Timer.Type.REALTIME))
            .addTimers(makeSerializedTimer(STATE_NAMESPACE_1, 1, Windmill.Timer.Type.WATERMARK))
            .addTimers(makeSerializedTimer(STATE_NAMESPACE_1, 2, Windmill.Timer.Type.REALTIME))
            .addTimers(makeSerializedTimer(STATE_NAMESPACE_2, 3, Windmill.Timer.Type.WATERMARK))
            .build())
        .build();

    KeyedWorkItem<String> keyedWorkItem =
        KeyedWorkItem.<String>workItem(KEY, workItem, WINDOW_CODER, WINDOWS_CODER, VALUE_CODER);

    assertThat(keyedWorkItem.timersIterable(), Matchers.contains(
        makeTimer(STATE_NAMESPACE_1, 1, TimeDomain.EVENT_TIME),
        makeTimer(STATE_NAMESPACE_2, 3, TimeDomain.EVENT_TIME),
        makeTimer(STATE_NAMESPACE_1, 0, TimeDomain.PROCESSING_TIME),
        makeTimer(STATE_NAMESPACE_1, 2, TimeDomain.PROCESSING_TIME)));
  }

  private static Windmill.Timer makeSerializedTimer(
      StateNamespace ns, long timestamp, Windmill.Timer.Type type) {
    return Windmill.Timer.newBuilder()
        .setTag(ByteString.copyFromUtf8(
            ns.stringKey() + "+" + type + "-" + timestamp))
        .setTimestamp(TimeUnit.MILLISECONDS.toMicros(timestamp))
        .setType(type)
        .setStateFamily(STATE_FAMILY)
        .build();
  }

  private static TimerData makeTimer(StateNamespace ns, long timestamp, TimeDomain domain) {
    return TimerData.of(ns, new Instant(timestamp), domain);
  }
}

