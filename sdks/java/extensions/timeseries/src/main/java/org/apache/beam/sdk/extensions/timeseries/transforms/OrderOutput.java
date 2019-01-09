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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
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
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
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
   * When a new key is seen (state == null) for the first time, we will create a timer that loops
   * until the TTL set in configuration has passed. In-between timers firing, we will add all new
   * elements to a List. lets have 3 elements coming in at various time and then there is NULL for
   * forth time slice [t1,t2,t3, NULL] t1 arrives and we set timer to fire at t1.
   *
   * <p>Then we have state.set(t1). t2 arrives and we add t1 to t2 as the previous value and we
   * output the value state.set(t2)
   *
   * <p>t3 arrives and we add t2 to t3 as the previous value and we output the value state.set(t3)
   *
   * <p>at time 4 we have no entry, here we use the last known state which is t3 and we change the
   * time values.
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
        Not supported in all runners
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

    @StateId("lastTimestampUpperBoundary")
    private final StateSpec<ValueState<Long>> lastTimestampUpperBoundary =
        StateSpecs.value(BigEndianLongCoder.of());

    @TimerId("alarm")
    private final TimerSpec alarm = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    /**
     * This is the simple path... A new element is here so we add it to the list of elements and set
     * a timer. As order is not guaranteed in a global window, we will need to ensure that we do not
     * rest the timer if the elements timestamp is > the current timer.
     */
    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("newElementsBag") BagState<TSAccum> newElementsBag,
        @StateId("lastTimestampUpperBoundary") ValueState<Long> lastTimestampUpperBoundary,
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
          c.element().getValue().getUpperWindowBoundary(),
          options.downSampleDuration());

      // Set the value for the highest observed upper boundary timestamp , this will be used to check the TTL on the timer
      setHighestTimestamp(
          lastTimestampUpperBoundary, c.element().getValue().getUpperWindowBoundary());
    }

    /**
     * This one is a little more complex...
     *
     * <p>There are two activities that happen in the OnTimer event
     *
     * <p>- Processing All current TSAccums in the BagState are read, ordered and added to the
     * processedTimeseriesList. The first item in the list is added to the the lastKnownValue. As
     * long as we have not exceeded the TTL for heartbeats we will set a timer one
     * downsample.duration away from timer.timestamp()
     *
     * <p>- Output We check to see if a processed TSAccum with
     * lowerWindowBoundary==timer.timestamp() exists. If it does exist we will set the previous
     * value to be lastKnownValue and then emit the TSAccum. The lastKnownValue is set to the
     * current value. If no TSAccum exists then we will emit a heartbeat value.
     */
    @OnTimer("alarm")
    public void onTimer(
        OnTimerContext c,
        @StateId("newElementsBag") BagState<TSAccum> newElementsBag,
        @StateId("currentTimerValue") ValueState<Long> currentTimerValue,
        //@StateId("processedTimeSeriesMap") MapState<Long,TSAccum> processedTimeSeriesMap,
        @StateId("processedTimeSeriesList") ValueState<List<TSAccum>> processedTimeSeriesList,
        @StateId("lastTimestampUpperBoundary") ValueState<Long> lastTimestampUpperBoundary,
        @TimerId("alarm") Timer timer,
        @StateId("lastKnownValue") ValueState<TSAccum> lastKnownValue) {

      // Check if we have more than one value in our list.
      List<TSAccum> newElements = Lists.newArrayList(newElementsBag.read());

      // Sort the list and then add to processedTimeSeriesList
      addElementsToOrderedProcessQueue(c, processedTimeSeriesList, newElements);

      LOG.debug("Timer Fire at {} Process Que size is {} last known value key {}  ",
          c.timestamp(),
          processedTimeSeriesList.read().size(),
          Optional.ofNullable(lastKnownValue.read()).orElse(TSAccum.newBuilder().build()).getKey());

      // Clear the elements now that they have been processed
      newElementsBag.clear();

      // Output the value stored in the the processed que which matches this timers time
      emitTSFromProcessedList(processedTimeSeriesList, options, lastKnownValue, c);

      // Check if this timer has already passed the time to live duration since the last call.
      if (withinTtl(lastTimestampUpperBoundary.read(), c.timestamp(), options)) {
        resetTimer(timer, currentTimerValue, options.downSampleDuration());
      } else {
        currentTimerValue.clear();
      }


    }
  }

  /**
   * Not all runners support MapState yet, we will use a ValueState as interim method. Replace once
   * MapState becomes available.
   *
   * @param processedTimeSeriesList
   * @param options
   * @param lastAccumState
   * @param context
   */
  //TODO This does not follow best practice patterns as it does a
  //read-modify-write with what could be a large list
  private static void emitTSFromProcessedList(
      ValueState<List<TSAccum>> processedTimeSeriesList,
      TSConfiguration options,
      ValueState<TSAccum> lastAccumState,
      DoFn.OnTimerContext context) {

    TSAccum lastValue = lastAccumState.read();
    List<TSAccum> processedTSAccums = processedTimeSeriesList.read();

    // If the lastAccum is Null this is the first time the Key has fired a timer series.
    // There will always be a value in the list for this situation
    if (lastValue == null) {
      TSAccum output = processedTSAccums.get(0);
      outputAccum(output, context, lastAccumState);
      removeFirstValueFromList(processedTSAccums, processedTimeSeriesList);
      return;
    }

    // If the list is empty then we emit a heartbeat timestamp
    // TODO check why we need to confirm size, if all paths lead to nulling of the list when done
    if (processedTSAccums == null || processedTSAccums.size()==0) {
      TSAccum heartBeat = heartBeat(lastValue, options);
      outputAccum(heartBeat, context, lastAccumState);
      return;
    }

    TSAccum output = processedTSAccums.get(0);

    // If timer is past the upper boundary of the first element something has gone wrong.
    checkTimerIsNotGreaterThanFirstValueInQueue(output,context);

    // If the list is not empty and the first value timestamp is > current timestamp then emit HB

    if (toMillis(output.getUpperWindowBoundary()) > context.timestamp().getMillis()) {
      TSAccum heartBeat = heartBeat(lastValue, options);
      outputAccum(heartBeat, context, lastAccumState);
      return;
    }

    // As the timestamp of the value in the Que matches our timestamp emit with previous value attached.
    // When saving back to lastAccum, use the original object

    TSAccum outputWithPrevious = setPreviousWindowValue(output, lastValue, context, processedTSAccums);

    context.outputWithTimestamp(
        KV.of(outputWithPrevious.getKey(), outputWithPrevious), context.timestamp());

    lastAccumState.write(output);

    removeFirstValueFromList(processedTSAccums, processedTimeSeriesList);
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
      ValueState<Long> lastTimestampUpperBoundary, com.google.protobuf.Timestamp timestamp) {

    Long obeservedTimestamp = Timestamps.toMillis(timestamp);

    if (Optional.ofNullable(lastTimestampUpperBoundary.read()).orElse(0L) < obeservedTimestamp) {
      lastTimestampUpperBoundary.write(obeservedTimestamp);
    }
  }
  // Check correctness

  private static void checkTimerIsNotGreaterThanFirstValueInQueue(TSAccum output, DoFn.OnTimerContext context ){
    if (toMillis(output.getUpperWindowBoundary()) < context.timestamp().getMillis()) {
      throw new IllegalStateException(
          String.format(
              "The first value in the list %s is smaller than our Timer %s. The value is %s",
              toMillis(output.getLowerWindowBoundary()),
              context.timestamp().getMillis(),
              output.toString()));
    }
  }
  private static void checkAccumState(TSAccum accum, Instant timestamp, List<TSAccum> processedQueue)
      throws IllegalStateException {

    if (accum.getPreviousWindowValue() == null) {
      return;
    }

    if (toMillis(accum.getLowerWindowBoundary())
        < toMillis(accum.getPreviousWindowValue().getUpperWindowBoundary())) {

      String errMsg = String.format("Accum has previous value with upper boundary greater "
              + "than existing lower boundary. "
              + "Timer Timestamp is %s Current Value Lower TS is %s "
              + "previous Value Upper TS is %s value of key is %s "
              + "current value is HB %s "
              + "previous value is HB %s",
          timestamp,
      Timestamps.toString(accum.getLowerWindowBoundary()),
          Timestamps.toString(accum.getPreviousWindowValue().getUpperWindowBoundary()),
          accum.getKey(),
          accum.containsMetadata(TSConfiguration.HEARTBEAT),
          accum.getPreviousWindowValue().containsMetadata(TSConfiguration.HEARTBEAT)
      );

      if(processedQueue!=null && processedQueue.size()>0){
        errMsg += String.format(" Size of process queue %s, first timestamp in processed queue %s"
            + " last timestamp in processed queue %s", processedQueue.size(), processedQueue.get(0),
            processedQueue.get(processedQueue.size()-1));
      }else{
        errMsg += String.format(" Size of process queue %s", 0);
      }

      throw new IllegalStateException(errMsg);
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

    heartBeat.setPreviousWindowValue(accum);
    heartBeat.putMetadata(TSConfiguration.HEARTBEAT, "");
    heartBeat.setLowerWindowBoundary(accum.getUpperWindowBoundary());
    heartBeat.setUpperWindowBoundary(
        Timestamps.add(
            accum.getUpperWindowBoundary(),
            Durations.fromMillis(options.downSampleDuration().getMillis())));

    return heartBeat.build();
  }

  private static boolean withinTtl(
      Long lastObservedUpperBoundaryTimestamp, Instant currentTimestamp, TSConfiguration options) {

    Instant ttlEnd = addDurationJoda(lastObservedUpperBoundaryTimestamp, options.timeToLive());

    return currentTimestamp.isBefore(ttlEnd);
  }

  private static Instant addDurationJoda(Long timestamp, Duration duration) {
    return new Instant(timestamp).plus(duration);
  }


  private static void addElementsToOrderedProcessQueue(DoFn.OnTimerContext c,
      ValueState<List<TSAccum>> processedTimeSeriesList, List<TSAccum> newElements) {

    if(newElements.isEmpty()){
      return;
    }

    List<TSAccum> accumList = processedTimeSeriesList.read();

    if (accumList == null) {
      accumList = new ArrayList<>();
    }

    if(LOG.isDebugEnabled()) {
      sortByUpperBoundary(newElements);

      LOG.info(
          "Timer firing at {} with fresh elements starting {} ending {}, Size of newElements list {} size processed que {} processed que exists {}",
          c.timestamp(), TSAccums.getTSAccumKeyWithPrettyTimeBoundary(newElements.get(0)),
          TSAccums.getTSAccumKeyWithPrettyTimeBoundary(newElements.get(newElements.size() - 1)),
          newElements.size(),
          accumList.size(),
          Optional.ofNullable(processedTimeSeriesList.read()).isPresent());
    }

    accumList.addAll(newElements);

    sortByUpperBoundary(accumList);

    processedTimeSeriesList.write(accumList);
  }

  private static TSAccum setPreviousWindowValue(
      TSAccum current, @Nullable TSAccum prev, DoFn.OnTimerContext context, List<TSAccum> processedTimeSeriesList) {

    TSAccum output =
        (prev == null) ? current : current.toBuilder().setPreviousWindowValue(prev).build();

    checkAccumState(output, context.timestamp(), processedTimeSeriesList);

    return output;
  }

  private static void outputAccum(
      TSAccum accum, DoFn.OnTimerContext context, ValueState<TSAccum> lastAccum) {

    context.outputWithTimestamp(KV.of(accum.getKey(), accum), context.timestamp());
    if (accum.hasPreviousWindowValue()) {
      lastAccum.write(accum.toBuilder().clearPreviousWindowValue().build());
    } else {
      lastAccum.write(accum);
    }
  }

  private static void removeFirstValueFromList(
      List<TSAccum> list, ValueState<List<TSAccum>> state) {

    list.remove(0);

    if (list.isEmpty()) {
      list.clear();
    } else {
      state.write(list);
    }
  }

  // ------- StateMap note available in all runners yet
  private static void setPreviousWindowValueAndAddToProcessedMap(
      MapState<Long, TSAccum> processedTimeSeriesMap,
      TSAccum current,
      @Nullable TSAccum prev,
      DoFn.OnTimerContext context) {

    TSAccum output =
        (prev == null) ? current : current.toBuilder().setPreviousWindowValue(prev).build();

 //   checkAccumState(output, context.timestamp());

    processedTimeSeriesMap.put(Timestamps.toMillis(output.getLowerWindowBoundary()), output);
  }

  /**
   * To be used when Runner supports MapState.
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
