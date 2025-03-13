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
package org.apache.beam.sdk.util.construction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Timer}. */
@RunWith(JUnit4.class)
public class TimerTest {
  private static final Instant FIRE_TIME = new Instant(123L);
  private static final Instant HOLD_TIME = new Instant(456L);

  @Test
  public void testClearTimer() {
    Timer<String> clearedTimer =
        Timer.cleared("timer", "tag", Collections.singleton(GlobalWindow.INSTANCE));
    assertTrue(clearedTimer.getClearBit());
    assertEquals("timer", clearedTimer.getUserKey());
    assertEquals("tag", clearedTimer.getDynamicTimerTag());
    assertEquals(Collections.singleton(GlobalWindow.INSTANCE), clearedTimer.getWindows());
  }

  @Test
  public void testTimer() {
    Timer<String> timer =
        Timer.of(
            "key",
            "tag",
            Collections.singleton(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING);
    assertEquals("key", timer.getUserKey());
    assertEquals("tag", timer.getDynamicTimerTag());
    assertEquals(FIRE_TIME, timer.getFireTimestamp());
    assertEquals(HOLD_TIME, timer.getHoldTimestamp());
    assertEquals(Collections.singleton(GlobalWindow.INSTANCE), timer.getWindows());
    assertEquals(PaneInfo.NO_FIRING, timer.getPane());
    assertFalse(timer.getClearBit());
  }

  @Test
  public void testTimerCoderWithInconsistentWithEqualsComponentCoders() throws Exception {
    Coder<Timer<String>> coder = Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    CoderProperties.coderSerializable(coder);
    CoderProperties.structuralValueDecodeEncodeEqual(
        coder,
        Timer.of(
            "key",
            "tag",
            Collections.singleton(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING));
    CoderProperties.structuralValueDecodeEncodeEqual(
        coder, Timer.cleared("key", "tag", Collections.singleton(GlobalWindow.INSTANCE)));
    CoderProperties.structuralValueConsistentWithEquals(
        coder,
        Timer.of(
            "key",
            "tag",
            Collections.singleton(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING),
        Timer.of(
            "key",
            "tag",
            Collections.singleton(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING));
    CoderProperties.structuralValueConsistentWithEquals(
        coder,
        Timer.cleared("key", "tag", Collections.singleton(GlobalWindow.INSTANCE)),
        Timer.cleared("key", "tag", Collections.singleton(GlobalWindow.INSTANCE)));
  }

  @Test
  public void testTimerCoderWithConsistentWithEqualsComponentCoders() throws Exception {
    Coder<Timer<String>> coder = Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    CoderProperties.coderDecodeEncodeEqual(
        coder,
        Timer.of(
            "key",
            "tag",
            Collections.singletonList(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING));
    CoderProperties.coderDecodeEncodeEqual(
        coder, Timer.cleared("key", "tag", Collections.singletonList(GlobalWindow.INSTANCE)));
    CoderProperties.coderConsistentWithEquals(
        coder,
        Timer.of(
            "key",
            "tag",
            Collections.singletonList(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING),
        Timer.of(
            "key",
            "tag",
            Collections.singletonList(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING));
    CoderProperties.coderConsistentWithEquals(
        coder,
        Timer.cleared("key", "tag", Collections.singletonList(GlobalWindow.INSTANCE)),
        Timer.cleared("key", "tag", Collections.singletonList(GlobalWindow.INSTANCE)));
    CoderProperties.coderDeterministic(
        coder,
        Timer.of(
            "key",
            "tag",
            Collections.singletonList(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING),
        Timer.of(
            "key",
            "tag",
            Collections.singletonList(GlobalWindow.INSTANCE),
            FIRE_TIME,
            HOLD_TIME,
            PaneInfo.NO_FIRING));
    CoderProperties.coderDeterministic(
        coder,
        Timer.cleared("key", "tag", Collections.singletonList(GlobalWindow.INSTANCE)),
        Timer.cleared("key", "tag", Collections.singletonList(GlobalWindow.INSTANCE)));
  }
}
