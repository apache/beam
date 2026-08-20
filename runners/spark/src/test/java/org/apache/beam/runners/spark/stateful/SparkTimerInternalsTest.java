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
package org.apache.beam.runners.spark.stateful;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link SparkTimerInternals}. */
public class SparkTimerInternalsTest {

  private static TimerData processingTimer(String timerId, Instant timestamp) {
    return TimerData.of(
        timerId,
        "",
        StateNamespaces.global(),
        timestamp,
        timestamp,
        TimeDomain.PROCESSING_TIME);
  }

  @Test
  public void testProcessingTimersFireInTimestampOrder() {
    SparkTimerInternals timerInternals = SparkTimerInternals.global(null);

    TimerData first = processingTimer("first", new Instant(1000));
    TimerData second = processingTimer("second", new Instant(2000));
    TimerData third = processingTimer("third", new Instant(3000));

    // Set out of order; firing order must follow the timestamps.
    timerInternals.setTimer(second);
    timerInternals.setTimer(third);
    timerInternals.setTimer(first);

    // Drain the way ParDoStateUpdateFn.SparkTimerInternalsIterator does.
    List<TimerData> fired = new ArrayList<>();
    TimerData timer;
    while ((timer = timerInternals.getNextProcessingTimer()) != null) {
      fired.add(timer);
      timerInternals.deleteTimer(timer);
    }

    assertEquals(ImmutableList.of(first, second, third), fired);
  }

  @Test
  public void testSettingATimerAgainClearsThePriorSetting() {
    SparkTimerInternals timerInternals = SparkTimerInternals.global(null);

    timerInternals.setTimer(processingTimer("timer", new Instant(1000)));
    TimerData latest = processingTimer("timer", new Instant(2000));
    timerInternals.setTimer(latest);

    assertEquals(ImmutableList.of(latest), ImmutableList.copyOf(timerInternals.getTimers()));
    assertEquals(latest, timerInternals.getNextProcessingTimer());
  }

  @Test
  public void testAddTimersKeepsTheLatestSettingOfATimer() {
    // State written before setTimer replaced prior settings can carry several settings of one
    // timer; the setting with the latest target wins regardless of restore order.
    TimerData earlier = processingTimer("timer", new Instant(1000));
    TimerData latest = processingTimer("timer", new Instant(2000));

    SparkTimerInternals timerInternals = SparkTimerInternals.global(null);
    timerInternals.addTimers(ImmutableList.of(earlier, latest).iterator());
    assertEquals(ImmutableList.of(latest), ImmutableList.copyOf(timerInternals.getTimers()));

    timerInternals = SparkTimerInternals.global(null);
    timerInternals.addTimers(ImmutableList.of(latest, earlier).iterator());
    assertEquals(ImmutableList.of(latest), ImmutableList.copyOf(timerInternals.getTimers()));
  }

  @Test
  public void testGetNextProcessingTimerIgnoresEventTimeTimers() {
    SparkTimerInternals timerInternals = SparkTimerInternals.global(null);
    timerInternals.setTimer(
        TimerData.of(
            "event", "", StateNamespaces.global(), new Instant(0), new Instant(0),
            TimeDomain.EVENT_TIME));

    assertNull(timerInternals.getNextProcessingTimer());
  }
}
