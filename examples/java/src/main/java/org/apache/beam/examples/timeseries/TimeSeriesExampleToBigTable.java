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

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.timeseries.GCPBigTableTimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.transforms.*;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccumSequences;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.extensions.timeseries.utils.TSMultiVariateDataPoints;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example pipeline is used to illustrate an advanced use of Keyed state and timers. The
 * pipeline extracts interesting information from timeseries data. One of the key elements, is the
 * transfer of data between fixed windows for a given key, as well as backfill when a key does not
 * have any new data within a time boundary. This sample should not be used in production. The
 * generated data is 5 identical waves with keys {Sin-1...Sin-5}, each data point has two values
 * {x,y}. x increments by 1 starting from 0 with a missed value in the series. { 0 .. 19 } - {10} y
 * is a simple f(x). The timestamp of the elements starts at 2018-01-01T00:00Z and increments one
 * sec for each x.
 *
 * <p>The output of this pipeline is to Google Cloud Bigtable.
 *
 * <p>The output of this pipeline is to a File path. Even though the aggregations for the x values
 * are are of little value as output, it is processed as part of this demo as it is useful when
 * eyeballing the results.
 */
public class TimeSeriesExampleToBigTable {

  static final Logger LOG = LoggerFactory.getLogger(TimeSeriesExampleToBigTable.class);

  static final String FILE_LOCATION = "/tmp/tf/";

  /** Push Timeseries data into BigTable */
  public static void main(String[] args) {

    // Create pipeline
    GCPBigTableTimeSeriesOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(GCPBigTableTimeSeriesOptions.class);
    /*
        TSConfiguration configuration =
            TSConfiguration.builder()
                .downSampleDuration(Duration.standardSeconds(5))
                .timeToLive(Duration.standardMinutes(1))
                .fillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE);
    */
    Pipeline p = Pipeline.create(options);

    // [START bigtable_dataflow_connector_config]

    CloudBigtableTableConfiguration config =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getProject())
            .withInstanceId(options.getBigTableInstanceId())
            .withTableId(options.getBigTableTableId())
            .build();

    // [END bigtable_dataflow_connector_config]

    // ------------ READ DATA ------------

    // Read some dummy timeseries data
    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>> readData =
        p.apply(Create.of(SinWaveSample.generateSinWave()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()));

    // ------------ Create perfect rectangles of data--------

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> downSampled =
        readData.apply(new ExtractAggregates()).apply(new GetWindowData());

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> weHaveOrder =
        downSampled.apply(new OrderOutput());

    // ------------ OutPut Data as Logs and TFRecords--------

    // This transform is purely to allow logged debug output, it will fail with OOM if large dataset is used.
    weHaveOrder.apply(new DebugSortedResult());
    // Write to Bigtable

    // tf.Example output
    weHaveOrder
        .apply(Values.create())
        .apply(new TSAccums.OutPutToBigTable())
        .apply(CloudBigtableIO.writeToTable(config));

    // Create 3 different window lengths for the TFSequenceExample
    weHaveOrder
        .apply(new TSAccumToFixedWindowSeq("Sequence of 1 Min", Duration.standardMinutes(1)))
        .apply(Values.create())
        .apply(new TSAccumSequences.OutPutToBigTable())
        .apply(CloudBigtableIO.writeToTable(config));

    weHaveOrder
        .apply(new TSAccumToFixedWindowSeq("Sequence of 5 Min", Duration.standardMinutes(5)))
        .apply(Values.create())
        .apply(new TSAccumSequences.OutPutToBigTable())
        .apply(CloudBigtableIO.writeToTable(config));

    weHaveOrder
        .apply(new TSAccumToFixedWindowSeq("Sequence of 15 Min", Duration.standardMinutes(15)))
        .apply(Values.create())
        .apply(new TSAccumSequences.OutPutToBigTable())
        .apply(CloudBigtableIO.writeToTable(config));

    p.run();
  }

  /** Simple data generator that creates some dummy test data for the timeseries examples. */
  public static class SinWaveSample {

    private static final Logger LOG = LoggerFactory.getLogger(SinWaveSample.class);

    public static List<TimeSeriesData.TSMultiVariateDataPoint> generateSinWave() {

      double y;
      double yBase = 1;
      double scale = 20;

      List<TimeSeriesData.TSMultiVariateDataPoint> dataPoints = new ArrayList<>();

      for (int k = 0; k < 10; k++) {

        Instant now = Instant.parse("2018-01-01T00:00Z");

        for (int i = 0; i < 20; i++) {

          if (!(i % 10 == 0)) {

            Instant dataPointTimeStamp = now.plus(Duration.standardSeconds(i));

            y = (yBase - Math.sin(Math.toRadians(i)) * scale);

            TimeSeriesData.TSMultiVariateDataPoint mvts =
                TimeSeriesData.TSMultiVariateDataPoint.newBuilder()
                    .setKey(TimeSeriesData.TSKey.newBuilder().setMajorKey("Sin-" + k).build())
                    .putData("x", TSDatas.createData(i))
                    .putData("y", TSDatas.createData(y))
                    .setTimestamp(Timestamps.fromMillis(dataPointTimeStamp.getMillis()))
                    .build();

            dataPoints.add(mvts);
          }
        }
      }

      return dataPoints;
    }
  }
}
