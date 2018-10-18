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

package org.apache.beam.examples.timeseries;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.transforms.ExtractAggregates;
import org.apache.beam.sdk.extensions.timeseries.transforms.GetValueFromKV;
import org.apache.beam.sdk.extensions.timeseries.transforms.OrderOutput;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.extensions.timeseries.utils.TSMultiVariateDataPoints;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for Timeseries examples. */
@RunWith(JUnit4.class)
public class TimeseriesTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TimeseriesTest.class);

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static final Instant NOW = Instant.parse("2018-01-01T00:00Z");
  private static final int MAJOR_KEY_COUNT = 1;
  private static final int ACCUM_COUNT = 6;
  private static final int WINDOW_DURATION = 10;

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

    TSConfiguration configuration =
        TSConfiguration.builder()
            .downSampleDuration(Duration.standardSeconds(WINDOW_DURATION))
            .timeToLive(Duration.standardSeconds(5))
            .fillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE)
            .build();

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply("Get Test Data", Create.of(SampleTimeseriesDataWithGaps.generateData(true, null)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply("GetValueFromKV", ParDo.of(new GetValueFromKV<>()))
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

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results = output.apply(ParDo.of(new GetValueFromKV<>()));

    PAssert.that(results).containsInAnyOrder(manualAccumListCreation());

    p.run();
  }

  @Test
  /*
   * This test will use data that has no missing ticks.
   */ public void checkEndToEndNoHeartBeat() {
    TSConfiguration configuration =
        TSConfiguration.builder()
            .downSampleDuration(Duration.standardSeconds(WINDOW_DURATION))
            .build();

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply("Get Test Data", Create.of(SampleTimeseriesDataWithGaps.generateData(true, null)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("GetValueFromKV", ParDo.of(new GetValueFromKV<>()))
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-1", Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply("Get Check Data", Create.of(loadPreviousValues(manualAccumListCreation())))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2", Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows()));

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results =
        output.apply(ParDo.of(new GetValueFromKV<String, TimeSeriesData.TSAccum>()));

    PAssert.that(results).containsInAnyOrder(loadPreviousValues(manualAccumListCreation()));

    p.run();
  }

  @Test
  /*
   * This test will use data that has missing sequences of ticks.
   * The result should still be a perfect rectangle of data for all of the key space.
   * TSAccum[1] Should be heartbeat values
   * TSAccum[1] Should have the values from TSAccum[0,3] inserted.
   */ public void checkEndToEndWithHeartBeat() {

    final Timestamp[] HEART_BEAT_VALUES =
        new Timestamp[] {
          Timestamps.add(
              Timestamps.fromMillis(NOW.getMillis()), Durations.fromSeconds(WINDOW_DURATION))
        };

    TSConfiguration configuration =
        TSConfiguration.builder()
            .fillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE)
            .timeToLive(Duration.standardSeconds(0))
            .downSampleDuration(Duration.standardSeconds(WINDOW_DURATION))
            .build();

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply(
                "Get Test Data",
                Create.of(SampleTimeseriesDataWithGaps.generateData(false, HEART_BEAT_VALUES)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("GetValueFromKV", ParDo.of(new GetValueFromKV<>()))
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply("Global-1", Window.into(new GlobalWindows()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply(
                "Get Check Data",
                Create.of(
                    removeTickRangesForHBTests(
                        loadPreviousValues(manualAccumListCreation()),
                        TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                        HEART_BEAT_VALUES)))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2", Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows()));

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results =
        output.apply(ParDo.of(new GetValueFromKV<String, TimeSeriesData.TSAccum>()));

    PAssert.that(results)
        .containsInAnyOrder(
            removeTickRangesForHBTests(
                loadPreviousValues(manualAccumListCreation()),
                TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                HEART_BEAT_VALUES));

    p.run();
  }

  @Test
  /*
   * This test will use data that has missing sequences of ticks.
   * The result should still be a perfect rectangle of data for all of the key space.
   * TSAccum[1,2,4] Should be heartbeat values
   * TSAccum[1,2,4] Should have the values from TSAccum[0,3] inserted.
   */ public void checkEndToEndWithMultipleHeartBeat() {

    Timestamp missing1 =
        Timestamps.add(
            Timestamps.fromMillis(NOW.getMillis()), Durations.fromSeconds(WINDOW_DURATION));

    Timestamp missing2 = Timestamps.add(missing1, Durations.fromSeconds(WINDOW_DURATION));

    Timestamp[] HEART_BEAT_VALUES = new Timestamp[] {missing1, missing2};

    TSConfiguration configuration =
        TSConfiguration.builder()
            .fillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE)
            .timeToLive(Duration.standardSeconds(0))
            .downSampleDuration(Duration.standardSeconds(WINDOW_DURATION))
            .build();

    PCollection<KV<String, TimeSeriesData.TSAccum>> output =
        p.apply(
                "Get Test Data",
                Create.of(SampleTimeseriesDataWithGaps.generateData(false, HEART_BEAT_VALUES)))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()))
            .apply(new ExtractAggregates())
            .apply(new OrderOutput())
            .apply("GetValueFromKV", ParDo.of(new GetValueFromKV<>()))
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_1",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply("Global-1", Window.into(new GlobalWindows()));

    // Output any differences here as difficult to debug with just Assert output on error

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums =
        p.apply(
                "Get Check Data",
                Create.of(
                    removeTickRangesForHBTests(
                        loadPreviousValues(manualAccumListCreation()),
                        TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                        HEART_BEAT_VALUES)))
            .apply(new TSAccums.OutputAccumWithTimestamp())
            .apply(
                "OutPutTSAccumAsKVWithPrettyTimeBoundary_2",
                ParDo.of(new TSAccums.OutPutTSAccumAsKVWithPrettyTimeBoundary()))
            .apply(
                "Global-2", Window.<KV<String, TimeSeriesData.TSAccum>>into(new GlobalWindows()));

    // Get keyed results:

    output.apply(new MatcheResults(tag1, tag2, manualCreatedAccums));

    PCollection<TimeSeriesData.TSAccum> results =
        output.apply(ParDo.of(new GetValueFromKV<String, TimeSeriesData.TSAccum>()));

    PAssert.that(results)
        .containsInAnyOrder(
            removeTickRangesForHBTests(
                loadPreviousValues(manualAccumListCreation()),
                TSConfiguration.BFillOptions.LAST_KNOWN_VALUE,
                HEART_BEAT_VALUES));

    p.run();
  }

  /** Simple data generator that creates some dummy test data for the timeseries examples. */
  private static class SampleTimeseriesDataWithGaps {

    /*
    Generate 3 major keys {Test-0,Test-1,Test-2}.
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

      if (!withOutDeletion) {
        List<TimeSeriesData.TSMultiVariateDataPoint> output = new ArrayList<>();
        for (TimeSeriesData.TSMultiVariateDataPoint dataPoint : dataPoints) {
          boolean delete = false;
          for (Timestamp t : hbTimestamps) {
            long dpt = Timestamps.toMillis(dataPoint.getTimestamp());
            long start = Timestamps.toMillis(t);
            long end =
                Timestamps.toMillis(Timestamps.add(t, Durations.fromSeconds(WINDOW_DURATION)));
            if (dpt >= start && dpt < end) {
              delete = true;
            }
          }
          if (!delete) {
            output.add(dataPoint);
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
              Timestamps.fromMillis(NOW.getMillis()), Durations.fromSeconds(i * WINDOW_DURATION));
      Timestamp upper =
          Timestamps.add(
              Timestamps.fromMillis(NOW.getMillis()),
              Durations.fromSeconds((i + 1) * WINDOW_DURATION));

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

    int countInWindow = WINDOW_DURATION;

    for (int i = 0; i < ACCUM_COUNT; i++) {

      int max = (i * WINDOW_DURATION) + WINDOW_DURATION - 1;
      int min = i * WINDOW_DURATION;
      int sum = (int) (((double) (max + min) / 2d) * (double) WINDOW_DURATION);

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

  private static List<TimeSeriesData.TSAccum> loadPreviousValues(
      List<TimeSeriesData.TSAccum> accums) {

    List<TimeSeriesData.TSAccum> output = new ArrayList<>();

    Map<String, List<TimeSeriesData.TSAccum>> keyMap = generateKeyMap(accums);

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

  private static List<TimeSeriesData.TSAccum> removeTickRangesForHBTests(
      List<TimeSeriesData.TSAccum> accums,
      TSConfiguration.BFillOptions fillOptions,
      Timestamp[] hbTimestamps) {

    List<TimeSeriesData.TSAccum> output = new ArrayList<>();

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

    Map<String, List<TimeSeriesData.TSAccum>> map = generateKeyMap(output);

    output.clear();

    for (List<TimeSeriesData.TSAccum> keyLists : map.values()) {

      // Sort list and then detect gaps and fill with heartbeat values
      TSAccums.sortByUpperBoundary(keyLists);

      Iterator<TimeSeriesData.TSAccum> accumIterator = keyLists.iterator();

      TimeSeriesData.TSAccum lastAccum = accumIterator.next();

      output.add(lastAccum);

      while (accumIterator.hasNext()) {

        TimeSeriesData.TSAccum next = accumIterator.next();

        while (Timestamps.comparator()
                .compare(next.getLowerWindowBoundary(), lastAccum.getUpperWindowBoundary())
            > 0) {

          TimeSeriesData.TSAccum.Builder outputPartial;

          if (fillOptions == TSConfiguration.BFillOptions.LAST_KNOWN_VALUE) {

            outputPartial = lastAccum.toBuilder();

          } else {

            outputPartial =
                lastAccum.toBuilder().clearDataAccum().clearFirstTimeStamp().clearLastTimeStamp();
          }

          // Set the previous value but only to depth n=1
          outputPartial.setPreviousWindowValue(lastAccum.toBuilder().clearPreviousWindowValue());

          outputPartial.putMetadata(TSConfiguration.HEARTBEAT, "");

          outputPartial.setLowerWindowBoundary(lastAccum.getUpperWindowBoundary());

          outputPartial.setUpperWindowBoundary(
              Timestamps.add(
                  lastAccum.getUpperWindowBoundary(), Durations.fromSeconds(WINDOW_DURATION)));

          output.add(outputPartial.build());

          lastAccum = outputPartial.build();
        }

        // Switch out previous window and assign
        if (lastAccum.getMetadataMap().containsKey(TSConfiguration.HEARTBEAT)) {
          TimeSeriesData.TSAccum changePreviousWindow =
              next.toBuilder()
                  .setPreviousWindowValue(lastAccum.toBuilder().clearPreviousWindowValue())
                  .build();
          lastAccum = changePreviousWindow;
          output.add(changePreviousWindow);
        } else {
          lastAccum = next;
          output.add(next);
        }
      }
    }

    return output;
  }

  private static class MatcheResults
      extends PTransform<PCollection<KV<String, TimeSeriesData.TSAccum>>, PDone> {

    TupleTag<TimeSeriesData.TSAccum> tag1;

    TupleTag<TimeSeriesData.TSAccum> tag2;

    PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums;

    public MatcheResults(
        TupleTag<TimeSeriesData.TSAccum> tag1,
        TupleTag<TimeSeriesData.TSAccum> tag2,
        PCollection<KV<String, TimeSeriesData.TSAccum>> manualCreatedAccums) {
      this.tag1 = tag1;
      this.tag2 = tag2;
      this.manualCreatedAccums = manualCreatedAccums;
    }

    @Override
    public PDone expand(PCollection<KV<String, TimeSeriesData.TSAccum>> input) {

      // Merge collection values into a CoGbkResult collection.
      PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(tag1, input)
              .and(tag2, manualCreatedAccums)
              .apply(CoGroupByKey.<String>create());

      coGbkResultCollection.apply(ParDo.of(new Match(tag1, tag2)));

      return PDone.in(input.getPipeline());
    }

    /** Use join to isolate missing items from Manual Test set and pipeline output. */
    private static class Match extends DoFn<KV<String, CoGbkResult>, String> {

      TupleTag<TimeSeriesData.TSAccum> pipelineOutput;

      TupleTag<TimeSeriesData.TSAccum> manualDataset;

      public Match(TupleTag<TimeSeriesData.TSAccum> tag1, TupleTag<TimeSeriesData.TSAccum> tag2) {
        this.pipelineOutput = tag1;
        this.manualDataset = tag2;
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow w) {

        KV<String, CoGbkResult> e = c.element();

        for (TimeSeriesData.TSAccum accum : e.getValue().getAll(manualDataset)) {

          Iterator<TimeSeriesData.TSAccum> pt2Val = e.getValue().getAll(pipelineOutput).iterator();

          if (pt2Val.hasNext()) {
            LOG.info(String.format(" Match found for Manual Dataset Item: %s ", e.getKey()));
            // Deep compare
            TimeSeriesData.TSAccum tsAccum2 = pt2Val.next();
            if (!accum.toByteString().equals(tsAccum2.toByteString())) {

              LOG.info(
                  String.format(
                      " Deep check failed with key %s manual value as accum 1 and pipeline value as accum  \n %s ",
                      e.getKey(), TSAccums.debugDetectOutputDiffBetweenTwoAccums(accum, tsAccum2)));
            }

            // If match found then no need to check for pipeline object
            return;

          } else {
            LOG.info(
                String.format(
                    " No match found for Manual Dataset Item: %s  window %s Accum %s",
                    e.getKey(), w, accum.toString()));
          }
        }
        // We will only be hear if there was no Manual pipeline option
        for (TimeSeriesData.TSAccum accum : e.getValue().getAll(pipelineOutput)) {
          LOG.info(
              String.format(
                  " No match found for Pipline Item: %s window %s Accum %s",
                  e.getKey(), w, accum.toString()));
        }
      }
    }
  }

  private static Map<String, List<TimeSeriesData.TSAccum>> generateKeyMap(
      List<TimeSeriesData.TSAccum> accums) {

    Map<String, List<TimeSeriesData.TSAccum>> keyMap = new HashMap<>();

    // Seperate all keys
    for (TimeSeriesData.TSAccum accum : accums) {
      String key = TSAccums.getTSAccumMajorMinorKeyAsString(accum);

      if (keyMap.containsKey(key)) {

        keyMap.get(key).add(accum);

      } else {
        keyMap.put(key, new ArrayList<>());
        keyMap.get(key).add(accum);
      }
    }

    return keyMap;
  }
}
