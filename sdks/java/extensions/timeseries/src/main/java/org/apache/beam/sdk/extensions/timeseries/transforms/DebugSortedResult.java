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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Debug Class to show results in logs, with larger datasets this will fail with OOM. */
@Experimental
public class DebugSortedResult
    extends PTransform<PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(DebugSortedResult.class);

  @Override
  public PDone expand(PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

    input
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<KV<TimeSeriesData.TSKey, Iterable<TimeSeriesData.TSAccum>>, String>() {

                  //@ProcessElement public void process(ProcessContext c, IntervalWindow w) {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    List<TimeSeriesData.TSAccum> accums = new ArrayList<>();

                    for (TimeSeriesData.TSAccum accum : c.element().getValue()) {
                      accums.add(accum);
                    }

                    TSAccums.sortAccumList(accums);

                    StringBuilder sb = new StringBuilder();

                    for (TimeSeriesData.TSAccum accum : accums) {
                      boolean heartBeat =
                          (accum.getMetadataMap().containsKey(TSConfiguration.HEARTBEAT));

                      sb.append("KEY : ")
                          .append(Optional.ofNullable(c.element().getKey().getMajorKey()))
                          .append(Optional.ofNullable(c.element().getKey().getMinorKeyString()))
                          .append("\n");

                      sb.append("\n- Is this a Heart Beat Value?  ")
                          .append(heartBeat)
                          .append("\n")
                          .append("- Window Start ")
                          .append(Timestamps.toString(accum.getLowerWindowBoundary()))
                          .append("\n")
                          .append("- Window End ")
                          .append(Timestamps.toString(accum.getUpperWindowBoundary()))
                          .append(
                              ((heartBeat)
                                  ? "\n Note as this is a HeartBeat first and last Timestamp values will be from last known value and will not match window boundaries.\n"
                                  : ""))
                          .append(" First TS - ")
                          .append(Timestamps.toString(accum.getFirstTimeStamp()))
                          .append(" Last TS - ")
                          .append(Timestamps.toString(accum.getLastTimeStamp()))
                          .append("\n")
                          .append(" First Data In Window - ")
                          .append(accum.getDataAccum().getFirst().getData().toString())
                          .append(" Last Data In Window - ")
                          .append(accum.getDataAccum().getLast().getData().toString())
                          .append("\n")
                          .append("- Count = ")
                          .append(accum.getDataAccum().getCount())
                          .append("\n")
                          .append("- Min = ")
                          .append(accum.getDataAccum().getMinValue())
                          .append("- Max = ")
                          .append(accum.getDataAccum().getMaxValue())
                          .append("\n")
                          .append("- Previous Upper Window Boundary = ")
                          .append(
                              Timestamps.toString(
                                  accum.getPreviousWindowValue().getUpperWindowBoundary()))
                          .append("\n")
                          .append("- Previous_Min = ")
                          .append(accum.getPreviousWindowValue().getDataAccum().getMinValue())
                          .append("- Previous_Max = ")
                          .append(accum.getPreviousWindowValue().getDataAccum().getMaxValue());
                      sb.append("\n\n");
                    }
                    LOG.info(sb.toString());
                  }
                }));

    return PDone.in(input.getPipeline());
  }
}
