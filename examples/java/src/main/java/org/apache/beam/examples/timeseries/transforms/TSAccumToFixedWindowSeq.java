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

package org.apache.beam.examples.timeseries.transforms;

import com.google.common.collect.Lists;
import org.apache.beam.examples.timeseries.Configuration.TSConfiguration;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.examples.timeseries.utils.TSAccums;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Converts a series of TSAccum into a TSAccumSequence.
 */
public class TSAccumToFixedWindowSeq extends
    PTransform<PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>, PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSAccumToFixedWindowSeq.class);

  TSConfiguration configuration;
  Duration fixedWindowDuration;

  public TSAccumToFixedWindowSeq(TSConfiguration configuration, Duration fixedWindowDuration) {
    this.configuration = configuration;
    this.fixedWindowDuration = fixedWindowDuration;
  }

  @Override public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

    return input.apply(Window.<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>into(
        FixedWindows.of(fixedWindowDuration))
        .withAllowedLateness(configuration.downSampleDuration().plus(Duration.millis(1)))
        .discardingFiredPanes()).apply(GroupByKey.create()).apply(ParDo
        .of(new DoFn<KV<TimeSeriesData.TSKey, Iterable<TimeSeriesData.TSAccum>>, KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccumSequence>>() {

          @ProcessElement public void processElement(ProcessContext c, IntervalWindow w) {

            List<TimeSeriesData.TSAccum> list = Lists
                .newArrayList(c.element().getValue().iterator());
            TSAccums.sortAccumList(list);

            TimeSeriesData.TSAccumSequence.Builder seq = TimeSeriesData.TSAccumSequence
                .newBuilder();

            seq.setKey(c.element().getKey());

            for (TimeSeriesData.TSAccum accum : list) {
              seq.addAccums(accum);
            }
            c.output(KV.of(c.element().getKey(), seq.build()));
          }
        }));

  }
}
