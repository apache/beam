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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GetWorkTimingInfosTrackerTest {

  @Test
  public void testGetWorkTimingInfosTracker_calculatesTransitToUserWorkerTimeFromWindmillWorker() {
    GetWorkTimingInfosTracker tracker = new GetWorkTimingInfosTracker(() -> 50);
    List<Windmill.GetWorkStreamTimingInfo> infos = new ArrayList<>();
    for (int i = 0; i <= 3; i++) {
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_CREATION_START)
              .setTimestampUsec(0)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_CREATION_END)
              .setTimestampUsec(10000)
              .build());
      tracker.addTimingInfo(infos);
      infos.clear();
    }
    // durations for each chunk:
    // GET_WORK_IN_WINDMILL_WORKER: 10, 10, 10, 10
    // GET_WORK_IN_TRANSIT_TO_USER_WORKER: 34, 33, 32, 31 -> sum to 130
    ImmutableList<Windmill.LatencyAttribution> attributions = tracker.getLatencyAttributions();
    assertEquals(2, attributions.size());
    Map<Windmill.LatencyAttribution.State, Windmill.LatencyAttribution> latencies =
        attributions.stream()
            .collect(toMap(Windmill.LatencyAttribution::getState, Function.identity()));

    assertEquals(
        10L,
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_WINDMILL_WORKER)
            .getTotalDurationMillis());

    assertEquals(
        // Elapsed time from 10 -> 50.
        40,
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_USER_WORKER)
            .getTotalDurationMillis());
  }

  @Test
  public void testGetWorkTimingInfosTracker_calculatesTransitToUserWorkerTimeFromDispatcher() {
    GetWorkTimingInfosTracker tracker = new GetWorkTimingInfosTracker(() -> 50);
    List<Windmill.GetWorkStreamTimingInfo> infos = new ArrayList<>();
    for (int i = 0; i <= 3; i++) {
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_CREATION_START)
              .setTimestampUsec(0)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_CREATION_END)
              .setTimestampUsec(10000)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_RECEIVED_BY_DISPATCHER)
              .setTimestampUsec((i + 11) * 1000)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_FORWARDED_BY_DISPATCHER)
              .setTimestampUsec((i + 16) * 1000)
              .build());
      tracker.addTimingInfo(infos);
      infos.clear();
    }
    // durations for each chunk:
    // GET_WORK_IN_WINDMILL_WORKER: 10, 10, 10, 10
    // GET_WORK_IN_TRANSIT_TO_DISPATCHER: 1, 2, 3, 4 -> sum to 10
    // GET_WORK_IN_TRANSIT_TO_USER_WORKER: 34, 33, 32, 31 -> sum to 130
    Map<Windmill.LatencyAttribution.State, Windmill.LatencyAttribution> latencies = new HashMap<>();
    ImmutableList<Windmill.LatencyAttribution> attributions = tracker.getLatencyAttributions();
    assertEquals(3, attributions.size());
    for (Windmill.LatencyAttribution attribution : attributions) {
      latencies.put(attribution.getState(), attribution);
    }
    assertEquals(
        10L,
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_WINDMILL_WORKER)
            .getTotalDurationMillis());
    // elapsed time from 10 -> 50;
    long elapsedTime = 40;
    // sumDurations: 1 + 2 + 3 + 4 + 34 + 33 + 32 + 31;
    long sumDurations = 140;
    assertEquals(
        Math.min(4, (long) (elapsedTime * (10.0 / sumDurations))),
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_DISPATCHER)
            .getTotalDurationMillis());
    assertEquals(
        Math.min(34, (long) (elapsedTime * (130.0 / sumDurations))),
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_USER_WORKER)
            .getTotalDurationMillis());
  }

  @Test
  public void testGetWorkTimingInfosTracker_clockSkew() {
    int skewMicros = 50 * 1000;
    GetWorkTimingInfosTracker tracker = new GetWorkTimingInfosTracker(() -> 50);
    List<Windmill.GetWorkStreamTimingInfo> infos = new ArrayList<>();
    for (int i = 0; i <= 3; i++) {
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_CREATION_START)
              .setTimestampUsec(skewMicros)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_CREATION_END)
              .setTimestampUsec(10000 + skewMicros)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_RECEIVED_BY_DISPATCHER)
              .setTimestampUsec((i + 11) * 1000 + skewMicros)
              .build());
      infos.add(
          Windmill.GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Windmill.GetWorkStreamTimingInfo.Event.GET_WORK_FORWARDED_BY_DISPATCHER)
              .setTimestampUsec((i + 16) * 1000 + skewMicros)
              .build());
      tracker.addTimingInfo(infos);
      infos.clear();
    }
    // durations for each chunk:
    // GET_WORK_IN_WINDMILL_WORKER: 10, 10, 10, 10
    // GET_WORK_IN_TRANSIT_TO_DISPATCHER: 1, 2, 3, 4 -> sum to 10
    // GET_WORK_IN_TRANSIT_TO_USER_WORKER: not observed due to skew
    Map<Windmill.LatencyAttribution.State, Windmill.LatencyAttribution> latencies = new HashMap<>();
    ImmutableList<Windmill.LatencyAttribution> attributions = tracker.getLatencyAttributions();
    assertEquals(2, attributions.size());
    for (Windmill.LatencyAttribution attribution : attributions) {
      latencies.put(attribution.getState(), attribution);
    }
    assertEquals(
        10L,
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_WINDMILL_WORKER)
            .getTotalDurationMillis());
    assertEquals(
        4L,
        latencies
            .get(Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_DISPATCHER)
            .getTotalDurationMillis());
    assertNull(latencies.get(Windmill.LatencyAttribution.State.GET_WORK_IN_TRANSIT_TO_USER_WORKER));
  }
}
