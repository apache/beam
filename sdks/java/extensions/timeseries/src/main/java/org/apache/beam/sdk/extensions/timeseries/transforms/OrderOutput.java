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

import static com.google.protobuf.util.Timestamps.toMillis;
import static org.apache.beam.sdk.extensions.timeseries.utils.TSAccums.sortByUpperBoundary;

import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.timeseries.TimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
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
        PCollection<KV<TimeSeriesData.TSKey, TSAccum>>,
        PCollection<KV<TimeSeriesData.TSKey, TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(OrderOutput.class);

  @Override
  public PCollection<KV<TimeSeriesData.TSKey, TSAccum>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TSAccum>> input) {

    TSConfiguration options =
        TSConfiguration.createConfigurationFromOptions(
            input.getPipeline().getOptions().as(TimeSeriesOptions.class));

    // Move into Global Time Domain, this allows Keyed State to retain its value across windows.
    // Late Data is dropped at this stage.

    PCollection<KV<TimeSeriesData.TSKey, TSAccum>> windowNoLateData =
        input.apply(
            "Global Window",
            Window.<KV<TimeSeriesData.TSKey, TSAccum>>into(new GlobalWindows())
                .withAllowedLateness(Duration.ZERO));

    return windowNoLateData
        .apply(ParDo.of(new GetPreviousData(options)))
        .apply(
            "Re Window post Global",
            Window.<KV<TimeSeriesData.TSKey, TSAccum>>into(
                    FixedWindows.of(options.downSampleDuration()))
                // TODO: DirectRunner not showing results with exact late date match
                // .withAllowedLateness(options.downSampleDuration().plus(options.downSampleDuration()))
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
   * before the timer fires, we will also hold a List of elements If the list of elements is greater
   * than 0 then we loop through the core logic until the list is exhausted.
   */
  @Experimental
  public static class GetPreviousData
      extends DoFn<KV<TimeSeriesData.TSKey, TSAccum>, KV<TimeSeriesData.TSKey, TSAccum>> {

    TSConfiguration options;

    private GetPreviousData(TSConfiguration configuration) {
      this.options = configuration;
    }

    // Setup our state objects

    @StateId("lastKnownValue")
    private final StateSpec<ValueState<TSAccum>> lastKnownValue =
        StateSpecs.value(ProtoCoder.of(TSAccum.class));

    @StateId("newElementsBag")
    private final StateSpec<BagState<TSAccum>> newElementsBag =
        StateSpecs.bag(ProtoCoder.of(TSAccum.class));

    @TimerId("alarm")
    private final TimerSpec alarm = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    /**
     * This is the simple path... A new element is here so we add it to the list of elements and set
     * a timer
     */
    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("newElementsBag") BagState<TSAccum> newElementsBag,
        @TimerId("alarm") Timer timer) {

      Instant alarm = c.timestamp().plus(options.downSampleDuration());

      newElementsBag.add(c.element().getValue());

      timer.set(alarm);

      LOG.debug("Setting timer.. " + alarm);
    }

    /**
     * This one is a little more complex...
     *
     * <p>In terms of timers, first we need to see if there are no new Accums, If not we check the
     * last heartbeat accum value and see if the current time minus time of last Accum is greater
     * than then max TTL We also now set the previous values into the current value.
     *
     * <pre>On first timer firing:
     *     - new elements will not be empty, will contain at least one element
     *     -that we saved there in processElement(); - last known value will be null;
     *
     * On any subsequent timer firing:
     *     - new elements can be empty (if no elements have been
     *       received since last timer firing), or not empty if there were elements received since
     *       last timer firing;
     *     - last known value will not be null because we have seen (and saved) at least one element
     *       during first timer firing;</pre>
     */
    @OnTimer("alarm")
    public void onTimer(
        OnTimerContext c,
        @StateId("newElementsBag") BagState<TSAccum> newElementsBag,
        @TimerId("alarm") Timer timer,
        @StateId("lastKnownValue") ValueState<TSAccum> lastKnownValue) {

      LOG.debug("Fire timer.. " + c.timestamp());

      TSAccum lastAccum = lastKnownValue.read();

      // Check if we have more than one value in our list.
      List<TSAccum> newElements = Lists.newArrayList(newElementsBag.read());

      // If there is no items in the list then output the last value with incremented timestamp
      // Read last value, change timestamp to now(), send output
      //
      // On first timer firing there will always be new elements that we saved in the state
      // in processElement()
      if (newElements.isEmpty()) {

        // So, if the list is empty it means that it is not the first timer firing,
        // and that means that we have seen some values before and thus last known value
        // cannot be null in this case.
        TSAccum heartBeat = heartBeat(lastAccum, options);
        setPreviousWindowValueAndEmit(heartBeat, lastAccum, c);
        lastAccum = heartBeat;
      } else {
        // If there are items in the list sort them and then process and output each element.
        // This can be either first or any subsequent timer firing.
        //
        // Work through all new elements since last timer firing
        for (TSAccum next : sortByUpperBoundary(newElements)) {

          // If there are gaps in the data points then we will fill with heartbeat values.
          while (lastAccum != null && hasGap(next, lastAccum)) {
            LOG.debug(
                "GAP FOUND! upper {} lower {}",
                next.getLowerWindowBoundary(),
                lastAccum.getUpperWindowBoundary());

            TSAccum heartBeat = heartBeat(lastAccum, options);
            setPreviousWindowValueAndEmit(heartBeat, lastAccum, c);
            lastAccum = heartBeat;
          }

          setPreviousWindowValueAndEmit(next, lastAccum, c);
          lastAccum = next;
        }
      }

      newElementsBag.clear();
      lastKnownValue.write(lastAccum);

      // Check if this timer has already passed the time to live duration since the last call.
      // Check if we are within the TTL for a key to emit.
      if (!withinTtl(lastAccum, c.timestamp(), options)) {
        resetTimer(timer, lastAccum.getUpperWindowBoundary(), options.downSampleDuration());
      }
    }
  }

  private static void resetTimer(Timer timer, Timestamp upperWindowBoundary, Duration duration) {
    Instant nextAlarm = addDurationJoda(upperWindowBoundary, duration);
    timer.set(nextAlarm);
  }

  // Check correctness
  private static void checkAccumState(TSAccum accum, Instant timestamp)
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
  private static TSAccum heartBeat(TSAccum accum, TSConfiguration options) {

    TSAccum.Builder heartBeat = accum.toBuilder();

    if (options.fillOption() != TSConfiguration.BFillOptions.LAST_KNOWN_VALUE) {
      heartBeat = accum.toBuilder().clearDataAccum().clearFirstTimeStamp().clearLastTimeStamp();
    }

    heartBeat.putMetadata(TSConfiguration.HEARTBEAT, "");
    heartBeat.setLowerWindowBoundary(accum.getUpperWindowBoundary());
    heartBeat.setUpperWindowBoundary(
        Timestamps.add(
            accum.getUpperWindowBoundary(),
            Durations.fromMillis(options.downSampleDuration().getMillis())));

    return heartBeat.build();
  }

  private static boolean withinTtl(
      TSAccum accum, Instant currentTimestamp, TSConfiguration options) {

    Instant ttlEnd =
        addDurationJoda(
            accum.getUpperWindowBoundary(),
            options.downSampleDuration().plus(options.timeToLive()));

    return currentTimestamp.isBefore(ttlEnd);
  }

  private static boolean hasGap(TSAccum next, TSAccum prev) {
    return Timestamps.comparator()
            .compare(next.getLowerWindowBoundary(), prev.getUpperWindowBoundary())
        != 0;
  }

  private static Instant addDurationJoda(Timestamp timestamp, Duration duration) {
    return new Instant(toMillis(timestamp)).plus(duration);
  }

  private static Instant toInstant(Timestamp timestamp) {
    return new Instant(toMillis(timestamp));
  }

  private static void setPreviousWindowValueAndEmit(
      TSAccum current, @Nullable TSAccum prev, DoFn.OnTimerContext context) {

    TSAccum output =
        (prev == null) ? current : current.toBuilder().setPreviousWindowValue(prev).build();
    checkAccumState(output, context.timestamp());

    context.outputWithTimestamp(
        KV.of(output.getKey(), output), toInstant(output.getLowerWindowBoundary()));
  }
}
