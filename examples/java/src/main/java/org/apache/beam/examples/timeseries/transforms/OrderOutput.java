/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.examples.timeseries.transforms;

import com.google.common.collect.Lists;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;

import java.util.Iterator;
import java.util.List;

import org.apache.beam.examples.timeseries.Configuration.TSConfiguration;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.examples.timeseries.utils.TSAccums;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create ordered output from the fixed windowed aggregations.
 */
@SuppressWarnings("serial") public class OrderOutput extends
    PTransform<PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>, PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(OrderOutput.class);

  TSConfiguration configuration;

  public OrderOutput(TSConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

    // Move into Global Time Domain, this allows Keyed State to retain its value across windows.
    // Late Data is dropped at this stage.

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> windowNoLateData = input.apply(
        Window.<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>into(new GlobalWindows())
            .withAllowedLateness(Duration.ZERO));
    return windowNoLateData.apply(ParDo.of(new GetPreviousData(configuration)));
  }

  /**
   * When a new key is seen (state == null) for the first time, we will create a timer to fire in the next window boundary.
   * If this is not the first time the key is seen we check the ttl to see if a new timer is required.
   * In-between timers firing, we will add all new elements to a List.
   * lets have 3 elements coming in at various time and then there is NULL for forth time slice [t1,t2,t3, NULL]
   * t1 arrives and we set timer to fire at t1.plus(downsample duration + fixed offset).
   * <p>
   * Then we have state.set(t1).
   * t2 arrives and we add t1 to t2 as the previous value and we output the value
   * state.set(t2)
   * <p>
   * t3 arrives and we add t2 to t3 as the previous value and we output the value
   * state.set(t3)
   * <p>
   * at time 4 we have no entry, here we use the last known state which is t3 and we change the time values.
   * <p>
   * NOTE: Need to see if this is really needed
   * In case there is more than one element arriving before the timer fires, we will also hold a List of elements
   * If the list of elements is > 0 then we loop through the core logic until the list is exhausted
   */
  public static class GetPreviousData extends
      DoFn<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>, KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> {

    TSConfiguration configuration;

    public GetPreviousData(TSConfiguration configuration) {
      this.configuration = configuration;
    }

    // Setup our state objects

    @StateId("lastKnownState") private final StateSpec<ValueState<TimeSeriesData.TSAccum>> lastKnownState = StateSpecs
        .value(ProtoCoder.of(TimeSeriesData.TSAccum.class));
    @StateId("holdingList") private final StateSpec<BagState<TimeSeriesData.TSAccum>> holdingList = StateSpecs
        .bag(ProtoCoder.of(TimeSeriesData.TSAccum.class));
    @TimerId("alarm") private final TimerSpec alarm = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    /**
     * This is the simple path... A new element is here so we add it to the list of elements and set a timer
     *
     * @param c
     * @param holdingList
     * @param timer
     */
    @ProcessElement public void processElement(ProcessContext c,
        @StateId("holdingList") BagState<TimeSeriesData.TSAccum> holdingList,
        @TimerId("alarm") Timer timer) {

      Instant alarm = c.timestamp().plus(configuration.downSampleDuration());

      timer.set(alarm);

      holdingList.add(c.element().getValue());
    }

    /**
     * This one is a little more complex...
     * <p>
     * In terms of timers, first we need to see if there are no new Accums,
     * If not we check the last heartbeat accum value  and see if the current time - time of last Accum is > then max TTL
     * We also now set the previous values into the current value
     *
     * @param c
     * @param holdingList
     * @param timer
     * @param lastKnownState
     */
    @OnTimer("alarm") public void onTimer(OnTimerContext c,
        @StateId("holdingList") BagState<TimeSeriesData.TSAccum> holdingList,
        @TimerId("alarm") Timer timer,
        @StateId("lastKnownState") ValueState<TimeSeriesData.TSAccum> lastKnownState) {

      //TODO: Refactor this function...

      // Check if we have more than one value in our list.

      Iterator<TimeSeriesData.TSAccum> it = holdingList.read().iterator();

      boolean newElements = it.hasNext();

      // If there is no items in the list then output the last value with incremented timestamp
      // Read last value, change timestamp to now(), send output
      TimeSeriesData.TSAccum lastAccum = lastKnownState.read();

      if (!newElements) {

        if (lastAccum != null) {

          // Get the last known value and create output from it with window incremented

          TimeSeriesData.TSAccum.Builder outputPartial = lastAccum.toBuilder();

          outputPartial.setLowerWindowBoundary(lastAccum.getUpperWindowBoundary());
          outputPartial.setUpperWindowBoundary(Timestamps.add(lastAccum.getUpperWindowBoundary(),
              Durations.fromMillis(configuration.downSampleDuration().getMillis())));

          // Set the last known state without previous window value
          lastKnownState.write(outputPartial.build());

          // Now set previous window value
          outputPartial.setPreviousWindowValue(lastAccum);
          outputPartial.putMetadata(TSConfiguration.HEARTBEAT, "");

          TimeSeriesData.TSAccum output = outputPartial.build();

          checkAccumOrderCorrectness(output, c.timestamp());

          c.outputWithTimestamp(KV.of(output.getKey(), output),
              new Instant(Timestamps.toMillis(output.getUpperWindowBoundary())));

        }

        // Check if this timer has already passed the time to live duration since the last call.

        if (configuration.fillOption() == TSConfiguration.BFillOptions.LAST_KNOWN_VALUE) {

          // Check if we are within the TTL for a key to emit.

          if (Timestamps.toMillis(Timestamps.add(lastAccum.getUpperWindowBoundary(),
              Durations.fromMillis(configuration.timeToLive().getMillis()))) > c.timestamp()
              .getMillis()) {
            return;

          }

          Instant alarm = new Instant(Timestamps.toMillis(lastAccum.getUpperWindowBoundary()))
              .plus(configuration.downSampleDuration());

          timer.set(alarm);

        }
        return;

      }

      // If there are items in the list sort them and then process and output each element

      if (newElements) {

        List<TimeSeriesData.TSAccum> accumList = Lists.newArrayList(holdingList.read());

        TSAccums.sortAccumList(accumList);

        Iterator<TimeSeriesData.TSAccum> listIterator = accumList.iterator();

        // Check if there is a already stored value, if not read from list and output
        if (lastAccum == null) {

          lastAccum = listIterator.next();

          lastKnownState.write(lastAccum);

          // If there was no stored accum then the first one has no previous
          TimeSeriesData.TSAccum output = lastAccum;

          checkAccumOrderCorrectness(output, c.timestamp());

          c.outputWithTimestamp(KV.of(output.getKey(), output),
              new Instant(Timestamps.toMillis(output.getUpperWindowBoundary())));
        }

        int i = 0;

        // Work through other elements

        while (listIterator.hasNext()) {

          ++i;

          TimeSeriesData.TSAccum next = listIterator.next();

          // Add current to next
          TimeSeriesData.TSAccum output = next.toBuilder().setPreviousWindowValue(lastAccum)
              .build();

          checkAccumOrderCorrectness(output, c.timestamp());

          c.outputWithTimestamp(KV.of(output.getKey(), output),
              new Instant(Timestamps.toMillis(output.getUpperWindowBoundary())));

          lastAccum = next;

        }

        if (LOG.isDebugEnabled() && i > 1) {
          LOG.debug(String.format("Key major %s minor  had %s entries at time %s",
              lastAccum.getKey().getMajorKey(), lastAccum.getKey().getMinorKeyString(), i,
              c.window()));
        }

        // Clear the list
        holdingList.clear();

        lastKnownState.write(lastAccum);

        Instant alarm = new Instant(Timestamps.toMillis(lastAccum.getUpperWindowBoundary()))
            .plus(configuration.downSampleDuration());

        timer.set(alarm);

      }

    }
  }

  // Check correctness
  public static void checkAccumOrderCorrectness(TimeSeriesData.TSAccum accum, Instant timestamp)
      throws IllegalStateException {

    if (accum.getPreviousWindowValue() == null) {
      return;
    }

    if (Timestamps.toMillis(accum.getLowerWindowBoundary()) < Timestamps
        .toMillis(accum.getPreviousWindowValue().getUpperWindowBoundary())) {

      LOG.error(String
          .format("Timer Timestamp is %s Current Value is %s previous is %s value of old is %s",
              timestamp, Timestamps.toString(accum.getLowerWindowBoundary()),
              Timestamps.toString(accum.getPreviousWindowValue().getUpperWindowBoundary()),
              accum.getKey()));

      throw new IllegalStateException(
          " Accum has previous value with uper boundary greater than existsing lower boundary");

    }
  }

}
