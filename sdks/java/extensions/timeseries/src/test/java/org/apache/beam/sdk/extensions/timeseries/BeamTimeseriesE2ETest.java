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

package org.apache.beam.sdk.extensions.timeseries;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.timeseries.TestUtils.MatcheResults;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSMultiVariateDataPoint;
import org.apache.beam.sdk.extensions.timeseries.transforms.ExtractAggregates;
import org.apache.beam.sdk.extensions.timeseries.transforms.OrderOutput;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.extensions.timeseries.utils.TSMultiVariateDataPoints;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TimeSeries test. */
@RunWith(JUnit4.class)
public class BeamTimeseriesE2ETest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BeamTimeseriesE2ETest.class);

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static final Instant NOW = Instant.parse("2018-01-01T00:00Z");
  private static final int MAJOR_KEY_COUNT = 1;
  private static final int ACCUM_COUNT = 6;
  private static final long WINDOW_DURATION_MILLS = 10000;
  private static final int WINDOW_DURATION_SEC = 10;

  // Create tuple tags for the value types in each collection.
  private static final TupleTag<TimeSeriesData.TSAccum> tag1 =
      new TupleTag<TimeSeriesData.TSAccum>("main") {};

  private static final TupleTag<TimeSeriesData.TSAccum> tag2 =
      new TupleTag<TimeSeriesData.TSAccum>("manual") {};

  @Test
  /*
   * Simple test of the aggregation functions, data will tick in every second no heartbeats.
   * The data will be downsampled to every 10 sec.
   * Data types tested: Double, Int, Float
   * Accum values SUM, COUNT, MIN, MAX, LAST, FIRST, TS_FIRST, TS_LAST
   * This should generate 6 TSAccums
   */ public void checkAggregationIntDoubleFloat() throws Exception {

    TimeSeriesOptions options = p.getOptions().as(TimeSeriesOptions.class);

    options.setDownSampleDurationMillis(WINDOW_DURATION_MILLS);
    options.setTimeToLiveMillis(0L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());

    TSConfiguration configuration = TSConfiguration.createConfigurationFromOptions(options);

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply("Get Test Data", Create.of(SampleTimeseriesDataWithGaps.generateData(true, null)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply("V1", Values.create())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply("Get Check Data", Create.of(manualAccumListCreation()))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(Window.into(FixedWindows.of(configuration.downSampleDuration())));

    // Get keyed results:

    output.apply(new TestUtils.MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results = output.apply("V2", Values.create());

    PAssert.that(results).containsInAnyOrder(manualAccumListCreation());

    p.run();
  }

  @Test
  /*
   * This test will use data that has no missing ticks.
   */ public void checkEndToEndNoHeartBeat() {

    TimeSeriesOptions options = p.getOptions().as(TimeSeriesOptions.class);

    options.setDownSampleDurationMillis(WINDOW_DURATION_MILLS);
    options.setTimeToLiveMillis(0L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply("Get Test Data", Create.of(SampleTimeseriesDataWithGaps.generateData(true, null)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("V1", Values.create())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-1",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply("Get Check Data", Create.of(loadPreviousValues(manualAccumListCreation())))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of()));

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results = output.apply("V2", Values.create());

    PAssert.that(results).containsInAnyOrder(loadPreviousValues(manualAccumListCreation()));

    p.run();
  }

  @Test
  /*
   * This test will use data that has missing sequences of ticks.
   * The result should still be a perfect rectangle of data for all of the key space.
   * TSAccum[1] Should be heartbeat values
   * TSAccum[1] Should have the values from TSAccum[0,3] inserted.
   */
  public void checkEndToEndWithHeartBeat() {
    TimeSeriesOptions options = p.getOptions().as(TimeSeriesOptions.class);

    options.setDownSampleDurationMillis(WINDOW_DURATION_MILLS);
    options.setTimeToLiveMillis(0L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());

    Timestamp missing1 =
        Timestamps.add(
            Timestamps.fromMillis(NOW.getMillis()), Durations.fromMillis(WINDOW_DURATION_MILLS));

    Timestamp[] heartBeatValues = new Timestamp[] {missing1};

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply(
                "Get Test Data",
                Create.of(SampleTimeseriesDataWithGaps.generateData(false, heartBeatValues)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("V1", Values.create())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-1",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply(
                "Get Check Data",
                Create.of(
                    removeTickRangesForHBTests(
                        loadPreviousValues(manualAccumListCreation()),
                        TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                        heartBeatValues)))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of()));

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results = output.apply("V2", Values.create());

    PAssert.that(results)
        .containsInAnyOrder(
            removeTickRangesForHBTests(
                loadPreviousValues(manualAccumListCreation()),
                TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                heartBeatValues));

    p.run();
  }

  @Ignore
  /*
   * This test will use data that has missing sequences of ticks.
   * The result should still be a perfect rectangle of data for all of the key space.
   * TSAccum[1] Should be heartbeat values
   * TSAccum[1] Should have the values from TSAccum[0,3] inserted.
   */
  public void checkEndToEndWithHeartBeatStream() {
    TimeSeriesOptions options = p.getOptions().as(TimeSeriesOptions.class);

    options.setDownSampleDurationMillis(WINDOW_DURATION_MILLS);
    options.setTimeToLiveMillis(10000L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());

    Timestamp missing1 =
        Timestamps.add(
            Timestamps.fromMillis(NOW.getMillis()), Durations.fromMillis(WINDOW_DURATION_MILLS));

    Timestamp[] heartBeatValues = new Timestamp[] {missing1};

    List<TimestampedValue<TSMultiVariateDataPoint>> dataPoints =
        TestUtils.convertTSMultiVariateDataPointToTimeStampedElement(
            SampleTimeseriesDataWithGaps.generateData(false, heartBeatValues));

    // Load first window of values

    TestStream.Builder<TSMultiVariateDataPoint> stream =
        TestStream.create(ProtoCoder.of(TSMultiVariateDataPoint.class));

    for (TimestampedValue<TSMultiVariateDataPoint> data : dataPoints.subList(0, 9)) {
      stream.addElements(data);
    }
    stream.advanceWatermarkTo(NOW.plus(Duration.millis(10000)));
    // Missing values expected from time 10 to 20 , so two watermark movements required.
    stream.advanceWatermarkTo(NOW.plus(Duration.millis(10000)));

    for (TimestampedValue<TSMultiVariateDataPoint> data : dataPoints.subList(10, 19)) {
      stream.addElements(data);
    }
    stream.advanceWatermarkTo(NOW.plus(Duration.millis(10000)));

    /*
        for ( TimestampedValue<TSMultiVariateDataPoint> data : dataPoints.subList(20,29)){
          stream.addElements(data);
        }
        stream    .advanceWatermarkTo(NOW.plus(Duration.millis(10000)));


        for ( TimestampedValue<TSMultiVariateDataPoint> data : dataPoints.subList(30,49)){
          stream.addElements(data);
        }
        stream    .advanceWatermarkTo(NOW.plus(Duration.millis(10000)));
    */

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply(stream.advanceWatermarkToInfinity())
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("V1", Values.create())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-1",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes());

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply(
                "Get Check Data",
                Create.of(
                    removeTickRangesForHBTests(
                        loadPreviousValues(manualAccumListCreation()),
                        TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                        heartBeatValues)))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes());

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results = output.apply("V2", Values.create());

    PAssert.that(results)
        .containsInAnyOrder(
            removeTickRangesForHBTests(
                loadPreviousValues(manualAccumListCreation()),
                TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                heartBeatValues));

    p.run();
  }

  @Test
  /*
   * This test will use data that has missing sequences of ticks.
   * The result should still be a perfect rectangle of data for all of the key space.
   * TSAccum[1,2,4] Should be heartbeat values
   * TSAccum[1,2,4] Should have the values from TSAccum[0,3] inserted.
   */
  public void checkEndToEndWithMultipleHeartBeat() {

    Timestamp missing1 =
        Timestamps.add(
            Timestamps.fromMillis(NOW.getMillis()), Durations.fromMillis(WINDOW_DURATION_MILLS));

    Timestamp missing2 = Timestamps.add(missing1, Durations.fromMillis(WINDOW_DURATION_MILLS));

    Timestamp[] heartBeatValues = new Timestamp[] {missing1, missing2};

    TimeSeriesOptions options = p.getOptions().as(TimeSeriesOptions.class);

    options.setDownSampleDurationMillis(WINDOW_DURATION_MILLS);
    options.setTimeToLiveMillis(0L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply(
                "Get Test Data",
                Create.of(SampleTimeseriesDataWithGaps.generateData(false, heartBeatValues)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("V1", Values.create())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-1",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply(
                "Get Check Data",
                Create.of(
                    removeTickRangesForHBTests(
                        loadPreviousValues(manualAccumListCreation()),
                        TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                        heartBeatValues)))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2",
                Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of()));

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results = output.apply("V2", Values.create());

    PAssert.that(results)
        .containsInAnyOrder(
            removeTickRangesForHBTests(
                loadPreviousValues(manualAccumListCreation()),
                TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                heartBeatValues));

    p.run();
  }

  /** Simple data generator that creates some dummy test data for the timeseries examples. */
  private static class SampleTimeseriesDataWithGaps {

    /*
    Generate MAJOR_KEY_COUNT major keys {Test-0,Test-1,Test-2}.
    Each key will have int value x, y values for Double and Float.
    Time will inc by 1 second at each tick for up to 60 ticks.

    All ticks between time range +10s and +20s as well as ticks between +40s and +50s will be omitted
     */
    private static List<TimeSeriesData.TSMultiVariateDataPoint> generateData(
        boolean withOutDeletion, Timestamp[] hbTimestamps) {

      List<TimeSeriesData.TSMultiVariateDataPoint> dataPoints = new ArrayList<>();

      for (int k = 0; k < MAJOR_KEY_COUNT; k++) {

        Instant now = Instant.parse("2018-01-01T00:00Z");

        for (int i = 0; i < 60; i++) {

          Instant dataPointTimeStamp = now.plus(Duration.standardSeconds(i));
          double y = (double) i;

          TimeSeriesData.TSMultiVariateDataPoint mvts =
              TimeSeriesData.TSMultiVariateDataPoint.newBuilder()
                  .setKey(TimeSeriesData.TSKey.newBuilder().setMajorKey("Test-" + k).build())
                  .putData("x-int", TimeSeriesData.Data.newBuilder().setIntVal(i).build())
                  .putData("x-double", TimeSeriesData.Data.newBuilder().setDoubleVal(y).build())
                  .putData(
                      "x-float", TimeSeriesData.Data.newBuilder().setFloatVal((float) y).build())
                  .setTimestamp(Timestamps.fromMillis(dataPointTimeStamp.getMillis()))
                  .build();

          dataPoints.add(mvts);
        }
      }

      // Remove values from the continuous list to enable gap filling
      if (!withOutDeletion) {
        List<TimeSeriesData.TSMultiVariateDataPoint> output = new ArrayList<>();
        for (TimeSeriesData.TSMultiVariateDataPoint dataPoint : dataPoints) {
          boolean delete = false;
          for (Timestamp t : hbTimestamps) {
            long dpt = Timestamps.toMillis(dataPoint.getTimestamp());
            long start = Timestamps.toMillis(t);
            long end =
                Timestamps.toMillis(Timestamps.add(t, Durations.fromMillis(WINDOW_DURATION_MILLS)));
            if (dpt >= start && dpt < end) {
              delete = true;
            }
          }
          if (!delete) {
            output.add(dataPoint);
          } else {
            LOG.info(
                "Deleting Accum from input list going into Pipeline : "
                    + (dataPoint.getTimestamp()));
          }
        }
        return output;
      } else {
        return dataPoints;
      }
    }
  }

  private static List<TimeSeriesData.TSAccum> manualAccumListCreation() {

    // Create map of all time series accums with numbers

    List<TimeSeriesData.TSAccum> accumTimelist = new ArrayList<TimeSeriesData.TSAccum>();

    // Generate all timestamps
    for (int i = 0; i < ACCUM_COUNT; i++) {
      Timestamp lower =
          Timestamps.add(
              Timestamps.fromMillis(NOW.getMillis()),
              Durations.fromMillis(i * WINDOW_DURATION_MILLS));
      Timestamp upper =
          Timestamps.add(
              Timestamps.fromMillis(NOW.getMillis()),
              Durations.fromMillis((i + 1) * WINDOW_DURATION_MILLS));

      accumTimelist.add(
          TimeSeriesData.TSAccum.newBuilder()
              .setFirstTimeStamp(lower)
              .setLastTimeStamp(Timestamps.subtract(upper, Durations.fromSeconds(1)))
              .setUpperWindowBoundary(upper)
              .setLowerWindowBoundary(lower)
              .build());
    }

    // Generate typed values

    List<TimeSeriesData.TSAccum> accumInt = new ArrayList<TimeSeriesData.TSAccum>();
    List<TimeSeriesData.TSAccum> accumDouble = new ArrayList<TimeSeriesData.TSAccum>();
    List<TimeSeriesData.TSAccum> accumFloat = new ArrayList<TimeSeriesData.TSAccum>();

    int countInWindow = WINDOW_DURATION_SEC;

    for (int i = 0; i < ACCUM_COUNT; i++) {

      int max = (i * WINDOW_DURATION_SEC) + (int) WINDOW_DURATION_SEC - 1;
      int min = i * (int) WINDOW_DURATION_SEC;
      int sum = (int) (((double) (max + min) / 2d) * (double) WINDOW_DURATION_SEC);

      accumInt.add(
          accumTimelist
              .get(i)
              .toBuilder()
              .setDataAccum(
                  TimeSeriesData.Accum.newBuilder()
                      .setCount(TSDatas.createData(countInWindow))
                      .setMaxValue(TSDatas.createData(max))
                      .setMinValue(TSDatas.createData(min))
                      .setSum(TSDatas.createData(sum)))
              .build());

      accumDouble.add(
          accumTimelist
              .get(i)
              .toBuilder()
              .setDataAccum(
                  TimeSeriesData.Accum.newBuilder()
                      .setCount(TSDatas.createData(countInWindow))
                      .setMaxValue(TSDatas.createData((double) max))
                      .setMinValue(TSDatas.createData((double) min))
                      .setSum(TSDatas.createData((double) sum)))
              .build());

      accumFloat.add(
          accumTimelist
              .get(i)
              .toBuilder()
              .setDataAccum(
                  TimeSeriesData.Accum.newBuilder()
                      .setCount(TSDatas.createData(countInWindow))
                      .setMaxValue(TSDatas.createData((float) max))
                      .setMinValue(TSDatas.createData((float) min))
                      .setSum(TSDatas.createData((float) sum)))
              .build());
    }

    // Set keys and First / Last values

    List<TimeSeriesData.TSAccum> allValues = new ArrayList<>();

    for (int i = 0; i < MAJOR_KEY_COUNT; i++) {

      for (TimeSeriesData.TSAccum accum : accumInt) {
        TimeSeriesData.TSKey key =
            TimeSeriesData.TSKey.newBuilder()
                .setMajorKey("Test-" + i)
                .setMinorKeyString("x-int")
                .build();

        allValues.add(setFirstAndLastValues(accum, key));
      }

      for (TimeSeriesData.TSAccum accum : accumDouble) {

        TimeSeriesData.TSKey key =
            TimeSeriesData.TSKey.newBuilder()
                .setMajorKey("Test-" + i)
                .setMinorKeyString("x-double")
                .build();

        // As the values are sequential in the Test data the following holds true
        allValues.add(setFirstAndLastValues(accum, key));
      }

      for (TimeSeriesData.TSAccum accum : accumFloat) {
        TimeSeriesData.TSKey key =
            TimeSeriesData.TSKey.newBuilder()
                .setMajorKey("Test-" + i)
                .setMinorKeyString("x-float")
                .build();

        allValues.add(setFirstAndLastValues(accum, key));
      }
    }

    return allValues;
  }

  /**
   * Given a genearetd list of Accum, add the previous window value
   *
   * @param accums
   * @return
   */
  private static List<TimeSeriesData.TSAccum> loadPreviousValues(
      List<TimeSeriesData.TSAccum> accums) {

    List<TimeSeriesData.TSAccum> output = new ArrayList<>();

    Map<String, List<TimeSeriesData.TSAccum>> keyMap = TestUtils.generateKeyMap(accums);

    for (List<TimeSeriesData.TSAccum> list : keyMap.values()) {
      // Sort List
      TSAccums.sortByUpperBoundary(list);
      // Walk list adding previous value to current

      Iterator<TimeSeriesData.TSAccum> accumIterator = list.iterator();

      TimeSeriesData.TSAccum lastvalue = accumIterator.next();
      output.add(lastvalue);

      while (accumIterator.hasNext()) {
        TimeSeriesData.TSAccum current = accumIterator.next();
        output.add(current.toBuilder().setPreviousWindowValue(lastvalue).build());
        lastvalue = current;
      }
    }
    return output;
  }

  private static TimeSeriesData.TSAccum setFirstAndLastValues(
      TimeSeriesData.TSAccum accumValue, TimeSeriesData.TSKey key) {

    TimeSeriesData.TSAccum.Builder accum = accumValue.toBuilder();

    // As the values are sequential in the Test data the following holds true
    TimeSeriesData.Data minData = accum.getDataAccum().getMinValue();
    TimeSeriesData.Data maxData = accum.getDataAccum().getMaxValue();
    Timestamp minTimeStamp = accum.getFirstTimeStamp();
    Timestamp maxTimeStamp = accum.getLastTimeStamp();

    accum.setKey(key);

    // Set the First & Last values in accum

    TimeSeriesData.TSDataPoint first =
        TimeSeriesData.TSDataPoint.newBuilder()
            .setData(minData)
            .setTimestamp(minTimeStamp)
            .setKey(key)
            .build();
    TimeSeriesData.TSDataPoint last =
        TimeSeriesData.TSDataPoint.newBuilder()
            .setData(maxData)
            .setTimestamp(maxTimeStamp)
            .setKey(key)
            .build();

    accum.setDataAccum(accum.getDataAccum().toBuilder().setFirst(first).setLast(last));

    return accum.build();
  }

  // Given a dummy dataset with gaps , fill with heartbeat values.
  private static List<TimeSeriesData.TSAccum> removeTickRangesForHBTests(
      List<TimeSeriesData.TSAccum> accums,
      TSConfiguration.BFillOptions fillOptions,
      Timestamp[] hbTimestamps) {

    List<TimeSeriesData.TSAccum> output = new ArrayList<>();

    // Remove every value which has a timestamp match in hbTimestamps
    for (Timestamp t : hbTimestamps) {

      output.clear();
      output.addAll(
          accums
              .stream()
              .filter(x -> (Timestamps.comparator().compare(t, x.getLowerWindowBoundary()) != 0))
              .collect(Collectors.toList()));

      accums.clear();
      accums.addAll(output);
    }

    Map<String, List<TimeSeriesData.TSAccum>> map = TestUtils.generateKeyMap(output);

    output.clear();

    // Sort through the list and fill any gaps found with HB values based on options.
    for (List<TimeSeriesData.TSAccum> keyLists : map.values()) {

      List<TimeSeriesData.TSAccum> keyTSAccumOuput = new ArrayList<>();
      List<TimeSeriesData.TSAccum> finalOutPut = new ArrayList<>();

      // Sort list and then detect gaps and fill with heartbeat values
      TSAccums.sortByUpperBoundary(keyLists);

      Iterator<TimeSeriesData.TSAccum> accumIterator = keyLists.iterator();

      TimeSeriesData.TSAccum lastAccum = accumIterator.next();

      keyTSAccumOuput.add(lastAccum);

      while (accumIterator.hasNext()) {

        TimeSeriesData.TSAccum next = accumIterator.next();

        while (Timestamps.comparator()
                .compare(next.getLowerWindowBoundary(), lastAccum.getUpperWindowBoundary())
            > 0) {

          TimeSeriesData.TSAccum.Builder generatedHeartbeatValue;

          // If LAST KNOWN VALUE is set then propagate the last value into the new.
          if (fillOptions == TSConfiguration.BFillOptions.LAST_KNOWN_VALUE) {

            generatedHeartbeatValue = lastAccum.toBuilder();

          } else {

            generatedHeartbeatValue =
                lastAccum.toBuilder().clearDataAccum().clearFirstTimeStamp().clearLastTimeStamp();
          }

          // Set the previous value but only to depth n=1
          generatedHeartbeatValue.setPreviousWindowValue(
              lastAccum.toBuilder().clearPreviousWindowValue());

          generatedHeartbeatValue.putMetadata(TSConfiguration.HEARTBEAT, "");

          generatedHeartbeatValue.setLowerWindowBoundary(lastAccum.getUpperWindowBoundary());

          generatedHeartbeatValue.setUpperWindowBoundary(
              Timestamps.add(
                  lastAccum.getUpperWindowBoundary(), Durations.fromMillis(WINDOW_DURATION_MILLS)));

          keyTSAccumOuput.add(generatedHeartbeatValue.build());

          LOG.info(
              String.format(
                  "Gap found in manual data between %s and %s added a value with lower time %s",
                  lastAccum.getUpperWindowBoundary(),
                  next.getLowerWindowBoundary(),
                  generatedHeartbeatValue.getLowerWindowBoundary()));

          lastAccum = generatedHeartbeatValue.build();
        }

        lastAccum = next;
        keyTSAccumOuput.add(next);
      }

      // As we have now changed values in the list we will reset the previousWindowValues;

      TSAccums.sortByUpperBoundary(keyTSAccumOuput);

      TimeSeriesData.TSAccum lastAccumValue = null;
      for (TimeSeriesData.TSAccum accum : keyTSAccumOuput) {
        if (lastAccumValue != null) {
          finalOutPut.add(
              accum
                  .toBuilder()
                  .setPreviousWindowValue(lastAccumValue.toBuilder().clearPreviousWindowValue())
                  .build());
        } else {
          finalOutPut.add(accum);
        }
        lastAccumValue = accum;
      }
      output.addAll(finalOutPut);
    }

    return output;
  }
}
