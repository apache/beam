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

package org.apache.beam.sdk.extensions.timeseries.transforms;

import com.google.common.collect.Lists;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.extensions.timeseries.TimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.annotations.Experimental;
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
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create ordered output from the fixed windowed aggregations. */
@SuppressWarnings("serial")
@Experimental
public class OrderOutput
    extends PTransform<
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>,
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(OrderOutput.class);


  @Override
  public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

    TSConfiguration options = TSConfiguration.createConfigurationFromOptions(input.getPipeline().getOptions().as(TimeSeriesOptions.class));

    // Move into Global Time Domain, this allows Keyed State to retain its value across windows.
    // Late Data is dropped at this stage.

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> windowNoLateData =
        input.apply(
            "Global Window",
            Window.<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                .withAllowedLateness(Duration.ZERO));

    return windowNoLateData
        .apply(ParDo.of(new GetPreviousData(options)))
        .apply(
            "Re Window post Global",
            Window.<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>into(
                    FixedWindows.of(options.downSampleDuration()))
                // TODO: DirectRunner not showing results with exact late date match
                //.withAllowedLateness(options.downSampleDuration().plus(options.downSampleDuration()))
                .withAllowedLateness(Duration.standardDays(1))
                .discardingFiredPanes());
  }

  /**
   * When a new key is seen (state == null) for the first time, we will create a timer to fire in
   * the next window boundary. If this is not the first time the key is seen we check the ttl to see
   * if a new timer is required. In-between timers firing, we will add all new elements to a List.
   * lets have 3 elements coming in at various time and then there is NULL for forth time slice
   * [t1,t2,t3, NULL] t1 arrives and we set timer to fire at t1.plus(downsample duration + fixed
   * offset).
   *
   * <p>Then we have state.set(t1). t2 arrives and we add t1 to t2 as the previous value and we
   * output the value state.set(t2)
   *
   * <p>t3 arrives and we add t2 to t3 as the previous value and we output the value state.set(t3)
   *
   * <p>at time 4 we have no entry, here we use the last known state which is t3 and we change the
   * time values.
   *
   * <p>NOTE: Need to see if this is really needed In case there is more than one element arriving
   * before the timer fires, we will also hold a List of elements If the list of elements is > 0
   * then we loop through the core logic until the list is exhausted
   */
  @Experimental
  public static class GetPreviousData
      extends DoFn<
          KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>,
          KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> {

    TSConfiguration options;

    private GetPreviousData(TSConfiguration configuration) {
      this.options = configuration;
    }

    // Setup our state objects

    @StateId("lastKnownState")
    private final StateSpec<ValueState<TimeSeriesData.TSAccum>> lastKnownState =
        StateSpecs.value(ProtoCoder.of(TimeSeriesData.TSAccum.class));

    @StateId("holdingList")
    private final StateSpec<BagState<TimeSeriesData.TSAccum>> holdingList =
        StateSpecs.bag(ProtoCoder.of(TimeSeriesData.TSAccum.class));

    @TimerId("alarm")
    private final TimerSpec alarm = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    /**
     * This is the simple path... A new element is here so we add it to the list of elements and set
     * a timer
     *
     * @param c
     * @param holdingList
     * @param timer
     */
    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("holdingList") BagState<TimeSeriesData.TSAccum> holdingList,
        @TimerId("alarm") Timer timer) {

      Instant alarm = c.timestamp().plus(options.downSampleDuration());

      timer.set(alarm);

      holdingList.add(c.element().getValue());

      LOG.debug("Setting timer.. " + alarm);
    }

    /**
     * This one is a little more complex...
     *
     * <p>In terms of timers, first we need to see if there are no new Accums, If not we check the
     * last heartbeat accum value and see if the current time - time of last Accum is > then max TTL
     * We also now set the previous values into the current value
     *
     * @param c
     * @param holdingList
     * @param timer
     * @param lastKnownState
     */
    @OnTimer("alarm")
    public void onTimer(
        OnTimerContext c,
        @StateId("holdingList") BagState<TimeSeriesData.TSAccum> holdingList,
        @TimerId("alarm") Timer timer,
        @StateId("lastKnownState") ValueState<TimeSeriesData.TSAccum> lastKnownState) {

      LOG.debug("Fire timer.. " + c.timestamp());

      // Check if we have more than one value in our list.

      Iterator<TimeSeriesData.TSAccum> it = holdingList.read().iterator();

      boolean newElements = it.hasNext();

      // If there is no items in the list then output the last value with incremented timestamp
      // Read last value, change timestamp to now(), send output
      TimeSeriesData.TSAccum lastAccum = lastKnownState.read();

      if (!newElements) {

        if (lastAccum != null) {
          // Set the last known state without previous window value

          TimeSeriesData.TSAccum.Builder heartBeatValue =
              generateHeartBeatAccum(lastAccum, options);

          lastKnownState.write(heartBeatValue.build());

          KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum> hbOutput =
              setPrevAndOutPutAccum(heartBeatValue, lastAccum, c.timestamp());

          c.outputWithTimestamp(
              hbOutput,
              new Instant(Timestamps.toMillis(hbOutput.getValue().getLowerWindowBoundary())));
        }

        // Check if this timer has already passed the time to live duration since the last call.

        // Check if we are within the TTL for a key to emit.

        if (Timestamps.toMillis(
                Timestamps.add(
                    lastAccum.getUpperWindowBoundary(),
                    Durations.fromMillis(
                        options
                            .downSampleDuration()
                            .plus(options.timeToLive())
                            .getMillis())))
            >= c.timestamp().getMillis()) {
          return;
        }

        Instant alarm =
            new Instant(Timestamps.toMillis(lastAccum.getUpperWindowBoundary()))
                .plus(options.downSampleDuration());

        timer.set(alarm);

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

          checkAccumState(output, c.timestamp());

          c.outputWithTimestamp(
              KV.of(output.getKey(), output),
              new Instant(Timestamps.toMillis(output.getLowerWindowBoundary())));
        }

        // Work through other elements

        while (listIterator.hasNext()) {

          TimeSeriesData.TSAccum next = listIterator.next();

          // If there are gaps in the data points then we will fill with heartbeat values.

          while (Timestamps.comparator()
                  .compare(next.getLowerWindowBoundary(), lastAccum.getUpperWindowBoundary())
              != 0) {

            LOG.debug(
                String.format(
                    "GAP FOUND! upper %s lower %s",
                    next.getLowerWindowBoundary(), lastAccum.getUpperWindowBoundary()));

            TimeSeriesData.TSAccum.Builder heartBeat =
                generateHeartBeatAccum(lastAccum, options);

            KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum> hbOutput =
                setPrevAndOutPutAccum(heartBeat, lastAccum, c.timestamp());

            c.outputWithTimestamp(
                hbOutput,
                new Instant(Timestamps.toMillis(hbOutput.getValue().getLowerWindowBoundary())));

            lastAccum = heartBeat.build();
          }

          KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum> hbOutput =
              setPrevAndOutPutAccum(next.toBuilder(), lastAccum, c.timestamp());

          c.outputWithTimestamp(
              hbOutput,
              new Instant(Timestamps.toMillis(hbOutput.getValue().getLowerWindowBoundary())));

          lastAccum = next;
        }

        // Clear the list
        holdingList.clear();

        lastKnownState.write(lastAccum);

        // Check if we are within the TTL for a key to emit.

        if (Timestamps.toMillis(
                Timestamps.add(
                    lastAccum.getUpperWindowBoundary(),
                    Durations.fromMillis(
                        options
                            .downSampleDuration()
                            .plus(options.timeToLive())
                            .getMillis())))
            >= c.timestamp().getMillis()) {
          return;
        }

        Instant alarm =
            new Instant(Timestamps.toMillis(lastAccum.getUpperWindowBoundary()))
                .plus(options.downSampleDuration());

        timer.set(alarm);
      }
    }
  }

  // Check correctness
  private static void checkAccumState(TimeSeriesData.TSAccum accum, Instant timestamp)
      throws IllegalStateException {

    if (accum.getPreviousWindowValue() == null) {
      return;
    }

    if (Timestamps.toMillis(accum.getLowerWindowBoundary())
        < Timestamps.toMillis(accum.getPreviousWindowValue().getUpperWindowBoundary())) {

      LOG.error(
          String.format(
              "Timer Timestamp is %s Current Value is %s previous is %s value of old is %s",
              timestamp,
              Timestamps.toString(accum.getLowerWindowBoundary()),
              Timestamps.toString(accum.getPreviousWindowValue().getUpperWindowBoundary()),
              accum.getKey()));

      throw new IllegalStateException(
          " Accum has previous value with uper boundary greater than existsing lower boundary");
    }
  }

  /**
   * Move the lower and upper boundary of the TS Accum by the options amount.
   *
   * @param accum accum that forms bases of heart beat.
   * @param options options to be used for options in heart beat creation.
   * @return TimeSeriesData.TSAccum.Builder
   */
  public static TimeSeriesData.TSAccum.Builder generateHeartBeatAccum(
      TimeSeriesData.TSAccum accum, TSConfiguration options) {

    TimeSeriesData.TSAccum.Builder outputPartial;

    if (options.fillOption() == TSConfiguration.BFillOptions.LAST_KNOWN_VALUE) {

      outputPartial = accum.toBuilder();

    } else {
      outputPartial = accum.toBuilder().clearDataAccum().clearFirstTimeStamp().clearLastTimeStamp();
    }

    outputPartial.putMetadata(TSConfiguration.HEARTBEAT, "");
    outputPartial.setLowerWindowBoundary(accum.getUpperWindowBoundary());
    outputPartial.setUpperWindowBoundary(
        Timestamps.add(
            accum.getUpperWindowBoundary(),
            Durations.fromMillis(options.downSampleDuration().getMillis())));

    return outputPartial;
  }

  public static KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum> setPrevAndOutPutAccum(
      TimeSeriesData.TSAccum.Builder currentAccum,
      TimeSeriesData.TSAccum prevAccum,
      Instant timeStamp) {

    TimeSeriesData.TSAccum output =
        TimeSeriesData.TSAccum.newBuilder(currentAccum.build())
            .setPreviousWindowValue(prevAccum)
            .build();

    checkAccumState(output, timeStamp);

    return KV.of(output.getKey(), output);
  }
}
