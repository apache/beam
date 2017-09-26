/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.examples.advanced.timeseries.pipelines;

import com.google.protobuf.Timestamp;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.examples.advanced.timeseries.configuration.TSConfiguration;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos.TSDataPoint;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos.TSDataPointSequence.Builder;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos.TSKey;
import org.apache.beam.examples.advanced.timeseries.transform.library.BackFillAllWindowsAndKeys;
import org.apache.beam.examples.advanced.timeseries.transform.library.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline demonstrates how you can backfill.
 */
public class TimeseriesFillPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(TimeseriesFillPipeline.class);

  @SuppressWarnings("serial")
  public static void main(String[] args) {

    // Setup start and end time for example Pipeline

    Instant startTime = new Instant("2000-01-01T00:00:00");

    Instant endTime = startTime.plus(Duration.standardSeconds(60));

    Duration downSampleDuration = Duration.standardSeconds(20);

    // Setup configuration
    TSConfiguration configuration = TSConfiguration.builder().setStartTime(startTime)
        .setEndTime(endTime).setDownSampleDuration(downSampleDuration)
        .setFillOption(TSConfiguration.FillOptions.NONE).setIsStreaming(false).build();

    // Create pipeline
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    // Define a view of all possible keys
    PCollectionView<List<String>> allKeys =
        p.apply(Create.of("TS1", "TS2", "TS3")).apply(View.asList());

    // Create 3 mock time series
    PCollection<TSDataPoint> dataPoints = p.apply(Create.of(createMockTimeSeries(configuration)));

    // As this example is not using a unbounded source which will set the
    // time stamp we will manually set it
    PCollection<TSDataPoint> dataPointsWithTimestamp = dataPoints.apply(Utils.extractTimeStamp());

    // Generate a tick for each window for each key even if there was no
    // data
    PCollection<TSDataPoint> dataPointsWithbackFill =
        dataPointsWithTimestamp.apply(new BackFillAllWindowsAndKeys(configuration, allKeys));

    dataPointsWithbackFill.apply(ParDo.of(new DoFn<TSDataPoint, KV<String, TSDataPoint>>() {

      // Print the output of the back fill
      // In order to print all data from all collections we need to
      // re-key and re-window into a Global Window

      @ProcessElement
      public void process(ProcessContext c) {
        c.output(KV.of(c.element().getKey().getKey(), c.element()));
      }
    })).apply(GroupByKey.create())
        .apply(ParDo.of(new DoFn<KV<String, Iterable<TSDataPoint>>, KV<String, Double>>() {

          @ProcessElement
          public void process(ProcessContext c, IntervalWindow w) {

            StringBuffer sb = new StringBuffer();

            sb.append(String.format("Key is %s Time Window is %s \n", c.element().getKey(),
                w.toString()));

            for (TSDataPoint ts : c.element().getValue()) {

              Builder list = TimeSeriesProtos.TSDataPointSequence.newBuilder();

              list.addAccums(ts);

              c.output(KV.of(c.element().getKey(), ts.getDoubleVal()));

              sb.append(String.format("Time is %s Value is %s  is heart beat? %s \n",
                  ts.getDoubleVal(), new Instant(ts.getTimestamp().getSeconds() * 1000),
                  ts.containsMetadata(TSConfiguration.HEARTBEAT)));
            }

            LOG.info(sb.toString());
          }
        })).apply(Sum.doublesPerKey())
        .apply(ParDo.of(new DoFn<KV<String, Double>, KV<String, Double>>() {

          // Print the per wind
          // In order to print all data from all collections we need to
          // re-key and re-window into a Global Window

          @ProcessElement
          public void process(ProcessContext c, IntervalWindow w) {

            LOG.info(String.format(" Sum of values for key %s in window %s was %s",
                c.element().getKey(), w.toString(), c.element().getValue()));

          }
        }));

    p.run();
  }

  /**
   * Create three mock time series with missing ticks.
   * @param configuration
   * @return List
   */
  public static List<TSDataPoint> createMockTimeSeries(TSConfiguration configuration) {

    // Create Time series
    List<TSDataPoint> ts1 = new ArrayList<>();

    double numElements =
        Math.floor(configuration.endTime().getMillis() - configuration.startTime().getMillis())
            / 5000;

    long startTime = configuration.startTime().getMillis();

    // Create a Data point every 5 seconds for TS1
    // Remove a data point at position 4,5,6,7
    for (long i = 0; i < numElements; i++) {
      if (i < 4 || i > 7) {
        ts1.add(TSDataPoint.newBuilder().setKey(TSKey.newBuilder().setKey("TS1")).setDoubleVal(i)
            .setTimestamp(Timestamp.newBuilder().setSeconds((startTime / 1000) + (5 * i))).build());
      }
    }

    // Create a Data point every 5 seconds for TS2
    // Remove data point at min 0
    for (long i = 0; i < numElements; i++) {
      if (i != 0) {
        ts1.add(TSDataPoint.newBuilder().setKey(TSKey.newBuilder().setKey("TS2"))
            .setDoubleVal(i + 100)
            .setTimestamp(Timestamp.newBuilder().setSeconds((startTime / 1000) + (5 * i))).build());
      }
    }

    // Add only one value at point 0
    ts1.add(TSDataPoint.newBuilder().setKey(TSKey.newBuilder().setKey("TS3")).setDoubleVal(1000)
        .setTimestamp(Timestamp.newBuilder().setSeconds((startTime / 1000))).build());

    return ts1;
  }

}
