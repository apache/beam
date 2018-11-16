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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.timeseries.FileSinkTimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.transforms.*;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccumSequences;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.extensions.timeseries.utils.TSMultiVariateDataPoints;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** This pipeline is a functional copy of TimeSeriesExampleToFile, with larger data output size. */
public class TimeSeriesExampleToFileLarge {

  public static void main(String[] args) {

    // Create pipeline
    FileSinkTimeSeriesOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FileSinkTimeSeriesOptions.class);

    options.setDownSampleDurationMillis(5000L);
    options.setTimeToLiveMillis(0L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());

    Pipeline p = Pipeline.create(options);

    // ------------ READ DATA ------------

    // Read some dummy timeseries data
    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>> readL1Data =
        p.apply(Create.of(SinWaveSample.generateSinWave()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()));

    // ------------ Create perfect rectangles of data--------

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> downSampled =
        readL1Data.apply(new ExtractAggregates());

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> weHaveOrder =
        //downSampled.apply(new OrderOutput());
        downSampled.apply(new OrderOutput());

    // ------------ OutPut Data as Logs and TFRecords--------

    weHaveOrder
        .apply(new TSAccumToFixedWindowSeq(Duration.standardMinutes(1)))
        .apply(Values.create())
        .apply(
            FileIO.<String, TimeSeriesData.TSAccumSequence>writeDynamic()
                .by(x -> TSAccumSequences.getTSAccumSequenceKeyMillsTimeBoundary(x))
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(FeatureDirectoryFileNaming::new)
                .via(
                    Contextful.fn(
                        new TimeSeriesExampleToFileLarge.TSAccumSequenceToExampleByteFn()),
                    TFRecordIO.sink())
                .to(options.getFileSinkDirectory() + "/tf/1min/tfSequence"));

    p.run();
  }

  /** Simple data generator that creates some dummy test data for the timeseries examples. */
  private static class SinWaveSample {

    private static List<TimeSeriesData.TSMultiVariateDataPoint> generateSinWave() {

      double y;
      double yBase = 1;
      double scale = 20;

      List<TimeSeriesData.TSMultiVariateDataPoint> dataPoints = new ArrayList<>();

      for (int k = 0; k < 1; k++) {

        Instant now = Instant.parse("2018-01-01T00:00Z");

        for (int i = 0; i < 3600; i++) {

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

      return dataPoints;
    }
  }

  static class TSAccumSequenceToExampleByteFn
      implements SerializableFunction<TimeSeriesData.TSAccumSequence, byte[]> {

    @Override
    public byte[] apply(TimeSeriesData.TSAccumSequence element) {
      byte[] returnVal;
      try {
        returnVal = TSAccumSequences.getSequenceExampleFromAccumSequence(element).toByteArray();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return returnVal;
    }
  }

  static class FeatureDirectoryFileNaming implements FileIO.Write.FileNaming {

    String partitionValue;

    public FeatureDirectoryFileNaming(String partitionValue) {
      this.partitionValue = partitionValue;
    }

    @Override
    public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {
      return String.format(
          "%s/%s-shard-%s-of-%s", this.partitionValue, this.partitionValue, shardIndex, numShards);
    }
  }
}
