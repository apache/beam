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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.timeseries.TimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.state.*;
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

/**
 * This transform takes Time series accumulators which are in a Fixed Window Time domain. Dependent
 * on options the transform is also able to : - fill any gaps in the data - propagate the previous
 * fixed window TSAccum value into the current TSAccum. TODO: Create Side Output to capture late
 * data
 */
@SuppressWarnings("serial")
@Experimental
@Deprecated
public class OrderOutput_v1
    extends PTransform<
        PCollection<KV<TimeSeriesData.TSKey, TSAccum>>,
        PCollection<KV<TimeSeriesData.TSKey, TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(OrderOutput_v1.class);

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
            "Global Window With Process Time output.",
            Window.<KV<TimeSeriesData.TSKey, TSAccum>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO));

    // Ensure all output is ordered and gaps are filed and previous values propagated
    return windowNoLateData
        .apply(ParDo.of(new GetPreviousData(options)))
        .apply(
            "Re Window post Global",
            Window.<KV<TimeSeriesData.TSKey, TSAccum>>into(
                    FixedWindows.of(options.downSampleDuration()))
                .triggering(Repeatedly.forever((AfterWatermark.pastEndOfWindow()))));
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
    /*
        @StateId("processedTimeSeriesMap")
        private final StateSpec<MapState<Long,TSAccum>> processedTimeSeriesMap =
            StateSpecs.map(BigEndianLongCoder.of(), ProtoCoder.of(TSAccum.class));
    */
    @StateId("processedTimeSeriesList")
    private final StateSpec<ValueState<List<TSAccum>>> processedTimeSeriesList =
        StateSpecs.value(ListCoder.<TSAccum>of(ProtoCoder.of(TSAccum.class)));

    @StateId("currentTimerValue")
    private final StateSpec<ValueState<Long>> currentTimerValue =
        StateSpecs.value(BigEndianLongCoder.of());

    @StateId("lastTimestampLowerBoundary")
    private final StateSpec<ValueState<Long>> lastTimestampLowerBoundary =
        StateSpecs.value(BigEndianLongCoder.of());

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
        @StateId("lastTimestampLowerBoundary") ValueState<Long> lastTimestampLowerBoundary,
        @StateId("currentTimerValue") ValueState<Long> currentTimerValue,
        @TimerId("alarm") Timer timer) {

      LOG.debug(
          "Adding TSAccum to Ordered Output {}",
          TSAccums.getTSAccumKeyWithPrettyTimeBoundary(c.element().getValue()));

      // Add the new TSAccum to list of accums to be processed on next timer to fire
      newElementsBag.add(c.element().getValue());

      // Set the timer if it has not already been set or if it has a higher value than the current TSAccum
      startTimer(
          timer,
          currentTimerValue,
          c.element().getValue().getLowerWindowBoundary(),
          options.downSampleDuration());

      // Set the value for the highest observed lower boundary timestamp , this will be used to check the TTL on the timer
      setHighestTimestamp(
          lastTimestampLowerBoundary, c.element().getValue().getLowerWindowBoundary());
    }

    /**
     * This one is a little more complex...
     *
     * <p>In terms of timers, first we need to see if there are no new Accums, If not we check the
     * last heartbeat accum value and see if the current time minus time of last Accum is greater
     * than the max TTL We also now set the previous values into the current value.
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
     *
     * Processed Accum's are transferred to a processed map where the key is the upper boundary of
     * the accum. On each Timer firing the value stored for the window is released to the next
     * stage.
     */
    @OnTimer("alarm")
    public void onTimer(
        OnTimerContext c,
        @StateId("newElementsBag") BagState<TSAccum> newElementsBag,
        @StateId("currentTimerValue") ValueState<Long> currentTimerValue,
        //@StateId("processedTimeSeriesMap") MapState<Long,TSAccum> processedTimeSeriesMap,
        @StateId("processedTimeSeriesList") ValueState<List<TSAccum>> processedTimeSeriesList,
        @StateId("lastTimestampLowerBoundary") ValueState<Long> lastTimestampLowerBoundary,
        @TimerId("alarm") Timer timer,
        @StateId("lastKnownValue") ValueState<TSAccum> lastKnownValue) {

      TSAccum lastAccum = lastKnownValue.read();

      // Check if we have more than one value in our list.
      List<TSAccum> newElements = Lists.newArrayList(newElementsBag.read());

      // If there is no items in the list then output the last value with incremented timestamp
      // Read last value, change timestamp to now(), send output
      //
      // On first timer firing there will always be new elements that we saved in the state
      // in processElement()
      if (!newElements.isEmpty()) {

        if (LOG.isDebugEnabled()) {
          sortByUpperBoundary(newElements);
          LOG.info(
              "Timer firing at {} with fresh elements starting {} ending {}",
              c.timestamp(),
              TSAccums.getTSAccumKeyWithPrettyTimeBoundary(newElements.get(0)),
              TSAccums.getTSAccumKeyWithPrettyTimeBoundary(
                  newElements.get(newElements.size() - 1)));
        }
        // If there are items in the list sort them and then process and output each element up to
        // the boundary of the timer's upper window.
        // This can be either first or any subsequent timer firing.
        // Work through all new elements since last timer firing

        LOG.debug("Size of newElements list {} ", newElements.size());

        for (TSAccum next : sortByUpperBoundary(newElements)) {

          //TODO remove gap fill as no longer needed
          // If there are gaps in the data points then we will fill with heartbeat values.
          while (lastAccum != null && hasGap(next, lastAccum)) {
            LOG.debug(
                "GAP FOUND! upper {} lower {} ",
                next.getLowerWindowBoundary(),
                lastAccum.getUpperWindowBoundary());

            TSAccum heartBeat = heartBeat(lastAccum, options);
            setPreviousWindowValueAndAddToProcessedList(
                processedTimeSeriesList, heartBeat, lastAccum, c);
            lastAccum = heartBeat;
          }

          setPreviousWindowValueAndAddToProcessedList(processedTimeSeriesList, next, lastAccum, c);
          lastAccum = next;
        }
      }

      lastKnownValue.write(lastAccum);

      newElementsBag.clear();

      // Output the value stored in the the processed que which matches this timers time
      emitTSFromProcessedList(processedTimeSeriesList, options, lastKnownValue, c);

      // Check if this timer has already passed the time to live duration since the last call.
      if (withinTtl(lastTimestampLowerBoundary.read(), c.timestamp(), options)) {
        LOG.debug(
            "Setting timer in OnTimer Code {} Found Accums Outside time boundary {} ",
            c.timestamp());
        resetTimer(timer, currentTimerValue, options.downSampleDuration());
      } else {
        currentTimerValue.clear();
      }
    }
  }

  // reset the timer
  private static void resetTimer(
      Timer timer, ValueState<Long> currentTimerValue, Duration duration) {

    Instant nextAlarm = addDurationJoda(currentTimerValue.read(), duration);
    timer.set(nextAlarm);
    currentTimerValue.write(nextAlarm.getMillis());
  }

  // Rest the timer, if it has not been currently set or the timestamp is higher then the current
  // timestamp.
  private static void startTimer(
      Timer timer,
      ValueState<Long> currentTimerValue,
      Timestamp upperWindowBoundary,
      Duration duration) {

    Instant nextAlarm = new Instant(toMillis(upperWindowBoundary));
    Long currentAlarm = currentTimerValue.read();

    if (currentAlarm == null) {
      currentAlarm = nextAlarm.getMillis();
    }

    if (currentAlarm >= nextAlarm.getMillis()) {
      timer.set(nextAlarm);
      currentTimerValue.write(nextAlarm.getMillis());
    }
  }

  // Set the value for the highest observed timestamp , this will be used to check the TTL on the timer
  public static void setHighestTimestamp(
      ValueState<Long> lastTimestampLowerBoundary, Timestamp timestamp) {

    Long obeservedTimestamp = Timestamps.toMillis(timestamp);

    if (Optional.ofNullable(lastTimestampLowerBoundary.read()).orElse(0L) < obeservedTimestamp) {
      lastTimestampLowerBoundary.write(obeservedTimestamp);
    }
  }
  // Check correctness
  private static void checkAccumState(TSAccum accum, Instant timestamp)
      throws IllegalStateException {

    if (accum.getPreviousWindowValue() == null) {
      return;
    }

    if (toMillis(accum.getLowerWindowBoundary())
        < toMillis(accum.getPreviousWindowValue().getUpperWindowBoundary())) {

      LOG.error(
          "Timer Timestamp is {} Current Value is {} previous is {} value of old is {}",
          timestamp,
          Timestamps.toString(accum.getLowerWindowBoundary()),
          Timestamps.toString(accum.getPreviousWindowValue().getUpperWindowBoundary()),
          accum.getKey());

      throw new IllegalStateException(
          " Accum has previous value with upper boundary greater than existing lower boundary");
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
      Long lastObservedLowerBoundaryTimestamp, Instant currentTimestamp, TSConfiguration options) {

    Instant ttlEnd = addDurationJoda(lastObservedLowerBoundaryTimestamp, options.timeToLive());

    return currentTimestamp.isBefore(ttlEnd);
  }

  private static boolean hasGap(TSAccum next, TSAccum prev) {

    int compare =
        Timestamps.comparator()
            .compare(prev.getUpperWindowBoundary(), next.getLowerWindowBoundary());

    if (compare > 0) {
      throw new IllegalStateException(
          String.format(
              "UpperBoundary of previous accum %s is greater than lower boundary of next accum %s.",
              TSAccums.getTSAccumKeyWithPrettyTimeBoundary(prev),
              TSAccums.getTSAccumKeyWithPrettyTimeBoundary(next)));
    }

    return compare != 0;
  }

  private static Instant addDurationJoda(Long timestamp, Duration duration) {
    return new Instant(timestamp).plus(duration);
  }

  private static Instant toInstant(Timestamp timestamp) {
    return new Instant(toMillis(timestamp));
  }

  private static void setPreviousWindowValueAndAddToProcessedList(
      ValueState<List<TSAccum>> processedTimeSeriesList,
      TSAccum current,
      @Nullable TSAccum prev,
      DoFn.OnTimerContext context) {

    TSAccum output =
        (prev == null) ? current : current.toBuilder().setPreviousWindowValue(prev).build();

    checkAccumState(output, context.timestamp());

    List<TSAccum> accumList = processedTimeSeriesList.read();

    if (accumList == null) {
      accumList = new ArrayList<>();
    }

    accumList.add(output);

    processedTimeSeriesList.write(accumList);
  }

  private static void setPreviousWindowValueAndAddToProcessedMap(
      MapState<Long, TSAccum> processedTimeSeriesMap,
      TSAccum current,
      @Nullable TSAccum prev,
      DoFn.OnTimerContext context) {

    TSAccum output =
        (prev == null) ? current : current.toBuilder().setPreviousWindowValue(prev).build();

    checkAccumState(output, context.timestamp());

    processedTimeSeriesMap.put(Timestamps.toMillis(output.getLowerWindowBoundary()), output);
  }

  /**
   * Not all runners support MapState yet, we will use a BagState as interim method. TODO This does
   * not follow best practice patterns as it does a read-modify-write with TODO what could be a
   * large list. Replace once MapState becomes available.
   *
   * @param processedTimeSeriesList
   * @param options
   * @param lastAccum
   * @param context
   */
  private static void emitTSFromProcessedList(
      ValueState<List<TSAccum>> processedTimeSeriesList,
      TSConfiguration options,
      ValueState<TSAccum> lastAccum,
      DoFn.OnTimerContext context) {

    List<TSAccum> processedTSAccums = processedTimeSeriesList.read();

    // If the list is empty then we emit a heartbeat timestamp
    if (processedTSAccums == null) {
      TSAccum heartBeat = heartBeat(lastAccum.read(), options);
      context.outputWithTimestamp(KV.of(heartBeat.getKey(), heartBeat), context.timestamp());
      lastAccum.write(heartBeat);
      return;
    }

    // As the que is not empty the first value we ready from it should match out Timers time

    TSAccum output = processedTSAccums.get(0);

    if (toMillis(output.getLowerWindowBoundary()) != context.timestamp().getMillis()) {
      throw new IllegalStateException(
          "The first value in the state list does not match our Timer.");
    }

    context.outputWithTimestamp(KV.of(output.getKey(), output), context.timestamp());

    processedTSAccums.remove(0);

    if (processedTSAccums.isEmpty()) {
      processedTimeSeriesList.clear();
    } else {
      processedTimeSeriesList.write(processedTSAccums);
    }
  }

  /**
   * To be used when Runner supports MapState
   *
   * @param processedTimeSeriesMap
   * @param options
   * @param lastAccum
   * @param context
   */
  private static void emitTSAccumFromProcessedMap(
      MapState<Long, TSAccum> processedTimeSeriesMap,
      TSConfiguration options,
      ValueState<TSAccum> lastAccum,
      DoFn.OnTimerContext context) {

    ReadableState<TSAccum> state = processedTimeSeriesMap.get(context.timestamp().getMillis());

    TSAccum output;

    // If we have a value in the processed Queue then emit, if not then we send out a HB message
    if ((output = state.read()) != null) {
      state.read();
      context.outputWithTimestamp(KV.of(output.getKey(), output), context.timestamp());

      // Remove now that it has been output
      processedTimeSeriesMap.remove(context.timestamp().getMillis());

    } else {
      TSAccum heartBeat = heartBeat(lastAccum.read(), options);
      context.outputWithTimestamp(KV.of(heartBeat.getKey(), heartBeat), context.timestamp());
      lastAccum.write(heartBeat);
    }
  }
}
