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
package org.apache.beam.runners.apex.translation.operators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.apex.translation.operators.ApexTimerInternals.TimerProcessor;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Tests for {@link ApexTimerInternals}.
 */
public class ApexTimerInternalsTest {

  @Test
  public void testEventTimeTimers() {

    final Map<String, Collection<TimerData>> firedTimers = new HashMap<>();
    TimerProcessor<String> timerProcessor = firedTimers::put;

    TimerDataCoder timerDataCoder = TimerDataCoder.of(GlobalWindow.Coder.INSTANCE);
    String key1 = "key1";
    Instant instant0 = new Instant(0);
    Instant instant1 = new Instant(1);
    Instant instant2 = new Instant(2);

    ApexTimerInternals<String> timerInternals = new ApexTimerInternals<>(timerDataCoder);
    timerInternals.setContext(key1, StringUtf8Coder.of(), Instant.now(), null);

    TimerData timerData0 = TimerData.of("timerData0", StateNamespaces.global(),
        instant0, TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timerData0);

    TimerData timerData1 = TimerData.of("timerData1", StateNamespaces.global(),
        instant1, TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timerData1);

    timerInternals.fireReadyTimers(instant0.getMillis(), timerProcessor, TimeDomain.EVENT_TIME);
    assertEquals(0, firedTimers.size());
    firedTimers.clear();

    timerInternals.fireReadyTimers(instant1.getMillis(), timerProcessor,
        TimeDomain.PROCESSING_TIME);
    assertEquals(0, firedTimers.size());
    timerInternals.fireReadyTimers(instant1.getMillis(), timerProcessor, TimeDomain.EVENT_TIME);
    assertEquals(1, firedTimers.size());
    assertEquals(Sets.newHashSet(timerData0),
        Sets.newHashSet(firedTimers.values().iterator().next()));
    firedTimers.clear();

    timerInternals.fireReadyTimers(instant2.getMillis(), timerProcessor, TimeDomain.EVENT_TIME);
    assertEquals(1, firedTimers.size());
    assertEquals(Sets.newHashSet(timerData1),
        Sets.newHashSet(firedTimers.values().iterator().next()));
    firedTimers.clear();
  }

  @Test
  public void testDeleteTimer() {
    TimerDataCoder timerDataCoder = TimerDataCoder.of(GlobalWindow.Coder.INSTANCE);
    String key1 = "key1";
    Instant instant0 = new Instant(0);
    Instant instant1 = new Instant(1);

    ApexTimerInternals<String> timerInternals = new ApexTimerInternals<>(timerDataCoder);
    timerInternals.setContext(key1, StringUtf8Coder.of(), Instant.now(), null);

    TimerData timerData0 = TimerData.of("timerData0", StateNamespaces.global(),
        instant0, TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timerData0);

    TimerData timerData1 = TimerData.of("timerData1", StateNamespaces.global(),
        instant1, TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timerData1);

    Map<?, Set<Slice>> timerMap = timerInternals.getTimerSet(TimeDomain.EVENT_TIME).getMap();
    assertEquals(1, timerMap.size());
    assertEquals(2, timerMap.values().iterator().next().size());

    timerInternals.deleteTimer(timerData0.getNamespace(), timerData0.getTimerId());
    assertEquals(1, timerMap.size());
    assertEquals(1, timerMap.values().iterator().next().size());

    timerInternals.deleteTimer(timerData1.getNamespace(), timerData1.getTimerId(),
        TimeDomain.PROCESSING_TIME);
    assertEquals(1, timerMap.size());
    assertEquals(1, timerMap.values().iterator().next().size());

    timerInternals.deleteTimer(timerData1.getNamespace(), timerData1.getTimerId(),
        TimeDomain.EVENT_TIME);
    assertEquals(0, timerMap.size());
  }

  @Test
  public void testSerialization() {
    TimerDataCoder timerDataCoder = TimerDataCoder.of(GlobalWindow.Coder.INSTANCE);
    TimerData timerData = TimerData.of("arbitrary-id", StateNamespaces.global(),
        new Instant(0), TimeDomain.EVENT_TIME);
    String key = "key";
    ApexTimerInternals<String> timerInternals = new ApexTimerInternals<>(timerDataCoder);
    timerInternals.setContext(key, StringUtf8Coder.of(), Instant.now(), null);
    timerInternals.setTimer(timerData);
    ApexTimerInternals<String> cloned;
    assertNotNull("Serialization", cloned = KryoCloneUtils.cloneObject(timerInternals));
    cloned.setContext(key, StringUtf8Coder.of(), Instant.now(), null);
    Map<?, Set<Slice>> timers = cloned.getTimerSet(TimeDomain.EVENT_TIME).getMap();
    assertEquals(1, timers.size());
  }

}
