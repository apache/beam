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
package org.apache.beam.examples.advanced.timeseries.transform.library;

import com.google.protobuf.Timestamp;

import java.util.List;

import org.apache.beam.examples.advanced.timeseries.configuration.TSConfiguration;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos.TSDataPoint;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos.TSKey;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates a Noop element. Streaming mode it will create a tick ever x time duration.
 * Batch mode it will create a window for every down sample Fixed window duration.
 */
@SuppressWarnings("serial")
@Experimental
public class BackFillAllWindowsAndKeys
    extends PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> {

  private static final Logger LOG = LoggerFactory.getLogger(BackFillAllWindowsAndKeys.class);

  TSConfiguration configuration;
  PCollectionView<List<String>> allKeys;

  /**
   * Fill any gaps in time series data per Key-Window combination. Window is based on the down
   * sample interval provided in the configuration All keys are passed as a PCollectioView to this
   * transform
   * @param configuration
   * @param allKeys
   */
  public BackFillAllWindowsAndKeys(TSConfiguration configuration,
      PCollectionView<List<String>> allKeys) {
    this.configuration = configuration;
    this.allKeys = allKeys;
  }

  @Override
  public PCollection<TSDataPoint> expand(PCollection<TSDataPoint> input) {

    // Window into down sample size
    PCollection<TSDataPoint> windowedMain =
        input.apply(Window.into(FixedWindows.of(configuration.downSampleDuration())));

    // Generate ticks within every window
    PCollection<Long> noop;

    if (configuration.isStreaming()) {
      noop = input.getPipeline()
          .apply(GenerateSequence.from(0).withRate(1, configuration.downSampleDuration())
              .withTimestampFn(new TimeFunction(configuration)));
    } else {
      if (configuration.endTime() == null) {
        throw new IllegalStateException("EndTime must be set in batch mode");
      }
      LOG.info(
          String.format("Creating windows from 0 to %s", Utils.calculateNumWindows(configuration)));

      noop = input.getPipeline()
          .apply(GenerateSequence.from(0).to(Utils.calculateNumWindows(configuration))
              .withTimestampFn(new TimeFunction(configuration)));
    }
    // In this step we create a dummy data point for each Fixed window
    // and key combination between starttime and endtime or infinite in stream mode
    PCollection<TSDataPoint> fillTimeSeries =
        noop.apply("Create Noop window ticks", ParDo.of(new DoFn<Long, TSDataPoint>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            for (String s : c.sideInput(allKeys)) {
              c.output(TSDataPoint.newBuilder().setKey(TSKey.newBuilder().setKey(s))
                  .setTimestamp(Timestamp.newBuilder().setSeconds(c.timestamp().getMillis() / 1000))
                  .putMetadata(TSConfiguration.HEARTBEAT, "true").build());
            }
            LOG.info(
                String.format("Creating HeartBeat for all values with generator %s timestamp %s",
                    c.element(), c.timestamp()));
          }
        }).withSideInputs(allKeys))
            .apply(Window.into(FixedWindows.of(configuration.downSampleDuration())));

    // Flatten the Noop's and the real data together
    PCollection<TSDataPoint> allValues =
        PCollectionList.of(windowedMain).and(fillTimeSeries).apply(Flatten.pCollections());

    // Apply Down sample window
    PCollection<TSDataPoint> allWindowsValues = allValues
        .apply(Window.<TSDataPoint>into(FixedWindows.of(configuration.downSampleDuration()))
            .withAllowedLateness(Duration.ZERO));
    return allWindowsValues;
  }

  /**
   * This function provides GenerateSequence with the time stamps it needs to attach to the
   * elements.
   */
  public static class TimeFunction implements SerializableFunction<Long, Instant> {

    Duration downSampleSize;
    TSConfiguration configuration;

    public TimeFunction(TSConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public Instant apply(Long input) {
      return configuration.startTime().plus(configuration.downSampleDuration().getMillis() * input);
    }

  }
}
