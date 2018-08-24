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

import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.examples.timeseries.Configuration.TSConfiguration;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.examples.timeseries.utils.TSAccums;
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

                      sb.append(
                          "KEY : "
                              + c.element().getKey().getMajorKey()
                              + c.element().getKey().getMinorKeyString()
                              + "\n");
                      sb.append(
                          "\n- Is this a Heart Beat Value?  "
                              + heartBeat
                              + "\n"
                              + "- Window Start "
                              + Timestamps.toString(accum.getLowerWindowBoundary())
                              + "\n"
                              + "- Window End "
                              + Timestamps.toString(accum.getUpperWindowBoundary())
                              + ((heartBeat)
                                  ? "\n Note as this is a HeartBeat first and last Timestamp values will be from last known value and will not match window boundaries.\n"
                                  : "")
                              + " First TS - "
                              + Timestamps.toString(accum.getFirstTimeStamp())
                              + " Last TS - "
                              + Timestamps.toString(accum.getLastTimeStamp())
                              + "\n"
                              + "- Count = "
                              + accum.getDataAccum().getCount()
                              + "\n"
                              + "- Min = "
                              + accum.getDataAccum().getMinValue()
                              + "- Max = "
                              + accum.getDataAccum().getMaxValue()
                              + "\n"
                              + "- Previous Upper Window Boundary = "
                              + Timestamps.toString(
                                  accum.getPreviousWindowValue().getUpperWindowBoundary())
                              + "\n"
                              + "- Previous_Min = "
                              + accum.getPreviousWindowValue().getDataAccum().getMinValue()
                              + "- Previous_Max = "
                              + accum.getPreviousWindowValue().getDataAccum().getMaxValue());
                      sb.append("\n\n");
                    }
                    LOG.info(sb.toString());
                  }
                }));

    return PDone.in(input.getPipeline());
  }
}
