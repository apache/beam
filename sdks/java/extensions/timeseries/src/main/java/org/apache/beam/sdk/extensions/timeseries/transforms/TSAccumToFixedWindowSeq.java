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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.TimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccumSequences;
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

/**
 * Converts a series of TSAccum into a TSAccumSequence. It is possible for the number of TSAccums to
 * be smaller than expected for a Fixed length window. This can happen in two situations: 1 - The
 * first time this key was seen was past the lower window boundary 2 - The time to live value was
 * set lower than the difference between the last value seen and the upper window boundary. Based on
 * various options this can either be ignored or corrected.
 *
 * <p>DiscardIncompleteSequences
 *
 * <p>ForwardPadSequenceOutput
 *
 * <p>BackPadSequenceOutput.
 *
 * <p>In the current version the sequence length % the downsample size must == 0
 *
 * <p>When doing backfill, it is important to note that the first value in the seq will not have the
 * value of previous window set.
 */
@Experimental
public class TSAccumToFixedWindowSeq
    extends PTransform<
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>,
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSAccumToFixedWindowSeq.class);

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
                    if (fixedWindowDuration.getMillis() % options.downSampleDuration().getMillis()
                        != 0) {
                      throw new IllegalStateException(
                          String.format(
                              "FixedWindowDuration %s must be cleanly divisible by DownSampleDuration %s.",
                              fixedWindowDuration, options.downSampleDuration()));
                    }
                  }

                  @ProcessElement
                  public void processElement(ProcessContext c, IntervalWindow w) {

                    // Check valid windows have been set

                    long windowStart = w.start().getMillis();
                    long windowEnd = w.end().getMillis();

                    com.google.protobuf.Timestamp lowerFixedWindowBoundary =
                        Timestamps.fromMillis(windowStart);
                    com.google.protobuf.Timestamp upperFixedWindowBoundary =
                        Timestamps.fromMillis(windowEnd);

                    List<TimeSeriesData.TSAccum> list =
                        Lists.newArrayList(c.element().getValue().iterator());

                    TSAccumSequences.applySequenceOuputRules(
                        options,
                        fixedWindowDuration,
                        list,
                        lowerFixedWindowBoundary,
                        upperFixedWindowBoundary);

                    TimeSeriesData.TSAccumSequence.Builder seq =
                        TimeSeriesData.TSAccumSequence.newBuilder();

                    seq.setKey(c.element().getKey());

                    seq.setLowerWindowBoundary(lowerFixedWindowBoundary);
                    seq.setUpperWindowBoundary(upperFixedWindowBoundary);
                    seq.setDuration(Durations.fromMillis(fixedWindowDuration.getMillis()));

                    seq.addAllAccums(list);

                    if (LOG.isDebugEnabled()) {

                      LOG.info(
                          "Created Sequence for time lower {} upper {} size {}",
                          Timestamps.toString(lowerFixedWindowBoundary),
                          Timestamps.toString(upperFixedWindowBoundary),
                          seq.getAccumsCount());

                      for (TSAccum a : seq.getAccumsList()) {
                        LOG.info(TSAccums.getTSAccumKeyWithPrettyTimeBoundary(a));
                      }
                    }

                    c.output(KV.of(c.element().getKey(), seq.build()));
                  }
                }));
  }
}
