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

import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Extract the window information from a TSAccum. */
@Experimental
public class GetWindowData
    extends PTransform<
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>,
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>> {

  @Override
  public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

    return input.apply(
        ParDo.of(
            new DoFn<
                KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>,
                KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>() {

              @ProcessElement
              public void process(ProcessContext c, IntervalWindow w) {
                c.output(
                    KV.of(
                        c.element().getKey(),
                        TimeSeriesData.TSAccum.newBuilder(c.element().getValue())
                            .setLowerWindowBoundary(Timestamps.fromMillis(w.start().getMillis()))
                            .setUpperWindowBoundary(Timestamps.fromMillis(w.end().getMillis()))
                            .build()));
              }
            }));
  }
}
