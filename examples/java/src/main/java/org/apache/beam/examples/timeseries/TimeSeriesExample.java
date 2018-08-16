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

import com.google.protobuf.util.Timestamps;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.examples.timeseries.Configuration.TSConfiguration;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.examples.timeseries.io.TF.TFExampleToBytes;
import org.apache.beam.examples.timeseries.io.TF.TFSequenceExampleToBytes;
import org.apache.beam.examples.timeseries.io.TF.TSAccumSequenceToTFSequencExample;
import org.apache.beam.examples.timeseries.io.TF.TSAccumToTFExample;
import org.apache.beam.examples.timeseries.transforms.DebugSortedResult;
import org.apache.beam.examples.timeseries.transforms.ExtractAggregates;
import org.apache.beam.examples.timeseries.transforms.GetValueFromKV;
import org.apache.beam.examples.timeseries.transforms.GetWindowData;
import org.apache.beam.examples.timeseries.transforms.OrderOutput;
import org.apache.beam.examples.timeseries.transforms.TSAccumToFixedWindowSeq;
import org.apache.beam.examples.timeseries.utils.TSMultiVarientDataPoints;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;

/**
 * This example pipeline is used to illustrate an advanced use of Keyed state and timers.
 * The pipeline extracts interesting information from timeseries data.
 * One of the key elements, is the transfer of data between fixed windows for a given key, as well as backfill
 * when a key does not have any new data within a time boundary.
 * This sample should not be used in production.
 */
public class TimeSeriesExample {

  static final String FILE_LOCATION = "/tmp/tf/";

  public static void main(String[] args) {

    // Create pipeline
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create()
        .as(PipelineOptions.class);

    TSConfiguration configuration = TSConfiguration.builder()
        .downSampleDuration(Duration.standardSeconds(5)).timeToLive(Duration.standardMinutes(1))
        .fillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE).build();

    Pipeline p = Pipeline.create(options);

    // ------------ READ DATA ------------

    // Read some dummy timeseries data
    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>> readL1Data = p
        .apply(Create.of(SinWaveSample.generateSinWave()))
        .apply(ParDo.of(new TSMultiVarientDataPoints.ExtractTimeStamp()))
        .apply(ParDo.of(new TSMultiVarientDataPoints.ConvertMultiToUniDataPoint()));

    // ------------ Create perfect rectangles of data--------

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> downSampled = readL1Data
        .apply(new ExtractAggregates(configuration)).apply(new GetWindowData());

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> weHaveOrder = downSampled
        .apply(new OrderOutput(configuration));

    // ------------ OutPut Data as Logs and TFRecords--------

    // This transform is purely to allow logged debug output, it will fail with OOM if large dataset is used.
    weHaveOrder.apply(new DebugSortedResult());

    // Write tfRecord to GCS, Single and Sequence, this is just for illustration
    // the data would not be useful in the existing format.
    weHaveOrder.apply(ParDo.of(new TSAccumToTFExample()))
        .apply(ParDo.of(new GetValueFromKV<Example>())).apply(ParDo.of(new TFExampleToBytes()))
        .apply(TFRecordIO.write().to(FILE_LOCATION + "raw/tfExamplerecords"));

    // Create 3 different window lengths for the TFSequenceExample
    weHaveOrder.apply(new TSAccumToFixedWindowSeq(configuration, Duration.standardMinutes(1)))
        .apply(ParDo.of(new TSAccumSequenceToTFSequencExample()))
        .apply(ParDo.of(new TFSequenceExampleToBytes()))
        .apply(TFRecordIO.write().to(FILE_LOCATION + "1min/tfSequenceExamplerecords"));

    weHaveOrder.apply(new TSAccumToFixedWindowSeq(configuration, Duration.standardMinutes(5)))
        .apply(ParDo.of(new TSAccumSequenceToTFSequencExample()))
        .apply(ParDo.of(new TFSequenceExampleToBytes()))
        .apply(TFRecordIO.write().to(FILE_LOCATION + "5min/tfSequenceExamplerecords"));

    weHaveOrder.apply(new TSAccumToFixedWindowSeq(configuration, Duration.standardMinutes(10)))
        .apply(ParDo.of(new TSAccumSequenceToTFSequencExample()))
        .apply(ParDo.of(new TFSequenceExampleToBytes()))
        .apply(TFRecordIO.write().to(FILE_LOCATION + "10min/tfSequenceExamplerecords"));

    p.run();

  }

  /**
   * Simple data generator that creates some dummy test data for the timeseries examples.
   */
  public static class SinWaveSample {

    private static final Logger LOG = LoggerFactory.getLogger(SinWaveSample.class);

    public static List<TimeSeriesData.TSMUltiVarientDataPoint> generateSinWave() {

      double y;
      double yBase = 1;
      double scale = 20;

      List<TimeSeriesData.TSMUltiVarientDataPoint> dataPoints = new ArrayList<>();

      for (int k = 0; k < 10; k++) {

        Instant now = Instant.parse("2018-01-01T00:00Z");

        for (int i = 0; i < 10000; i++) {

          if (!((i % 10 == 0))) {

            Instant dataPointTimeStamp = now.plus(Duration.standardSeconds(i));

            y = (yBase - Math.sin(Math.toRadians(i)) * scale);

            TimeSeriesData.TSMUltiVarientDataPoint mvts = TimeSeriesData.TSMUltiVarientDataPoint
                .newBuilder()
                .setKey(TimeSeriesData.TSKey.newBuilder().setMajorKey("Sin-" + k).build())
                .putData("x", TimeSeriesData.Data.newBuilder().setIntVal(i).build())
                .putData("y", TimeSeriesData.Data.newBuilder().setDoubleVal(y).build())
                .setTimestamp(Timestamps.fromMillis(dataPointTimeStamp.getMillis())).build();

            dataPoints.add(mvts);

          }
        }
      }

      return dataPoints;
    }
  }

}
