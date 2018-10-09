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

import com.google.common.collect.Lists;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.TimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts a series of TSAccum into a TSAccumSequence. */
@Experimental
public class TSAccumToFixedWindowSeq
    extends PTransform<
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>,
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSAccumToFixedWindowSeq.class);

  TSConfiguration configuration;
  private Duration fixedWindowDuration;

  public TSAccumToFixedWindowSeq(Duration fixedWindowDuration) {
    this.fixedWindowDuration = fixedWindowDuration;
  }

  public TSAccumToFixedWindowSeq(@Nullable String name, Duration fixedWindowDuration) {
    super(name);
    this.fixedWindowDuration = fixedWindowDuration;
  }

  @Override
  public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

    return input
        .apply(Window.into(FixedWindows.of(fixedWindowDuration)))
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<
                    KV<TimeSeriesData.TSKey, Iterable<TimeSeriesData.TSAccum>>,
                    KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>>() {

                  TSConfiguration options;

                  @StartBundle
                  public void startBundle(StartBundleContext c) {
                    options =
                        TSConfiguration.createConfigurationFromOptions(
                            c.getPipelineOptions().as(TimeSeriesOptions.class));
                  }

                  @ProcessElement
                  public void processElement(ProcessContext c, IntervalWindow w) {

                    com.google.protobuf.Timestamp lowerBoundary = null;
                    com.google.protobuf.Timestamp upperBoundary =
                        com.google.protobuf.Timestamp.newBuilder().build();

                    // TODO switch to buffered sorter

                    List<TimeSeriesData.TSAccum> list =
                        Lists.newArrayList(c.element().getValue().iterator());
                    TSAccums.sortAccumList(list);

                    TimeSeriesData.TSAccumSequence.Builder seq =
                        TimeSeriesData.TSAccumSequence.newBuilder();

                    seq.setKey(c.element().getKey());

                    TimeSeriesData.TSAccum testPrev = null;

                    for (TimeSeriesData.TSAccum accum : list) {

                      if (lowerBoundary == null) {
                        lowerBoundary = accum.getLowerWindowBoundary();
                      } else {
                        lowerBoundary =
                            (Timestamps.comparator()
                                        .compare(lowerBoundary, accum.getLowerWindowBoundary())
                                    < 0
                                ? lowerBoundary
                                : accum.getLowerWindowBoundary());
                      }

                      upperBoundary =
                          (Timestamps.comparator()
                                      .compare(upperBoundary, accum.getUpperWindowBoundary())
                                  > 0
                              ? upperBoundary
                              : accum.getUpperWindowBoundary());

                      if (options.fillOption() != TSConfiguration.BFillOptions.NONE) {
                        Optional.ofNullable(testPrev)
                            .ifPresent(
                                x -> {
                                  if (Durations.toMillis(
                                          Timestamps.between(
                                              x.getUpperWindowBoundary(),
                                              accum.getLowerWindowBoundary()))
                                      != 0) {

                                    LOG.warn(
                                        "Gap detected in sequence but Back Fill Option is not set to NONE");
                                  }
                                });
                      }

                      seq.addAccums(accum);
                    }

                    if (lowerBoundary == null) {
                      lowerBoundary = com.google.protobuf.Timestamp.newBuilder().build();
                    }

                    seq.setLowerWindowBoundary(lowerBoundary);
                    seq.setUpperWindowBoundary(upperBoundary);
                    seq.setDuration(Durations.fromMillis(fixedWindowDuration.getMillis()));

                    c.output(KV.of(c.element().getKey(), seq.build()));
                  }
                }));
  }
}
