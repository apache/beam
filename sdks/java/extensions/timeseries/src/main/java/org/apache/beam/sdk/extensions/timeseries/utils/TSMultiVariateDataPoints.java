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

package org.apache.beam.sdk.extensions.timeseries.utils;

import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;

/** Utility functions for TSMultiVarientDataPoint. */
@Experimental
public class TSMultiVariateDataPoints {

  public static class ConvertMultiToUniDataPoint
      extends DoFn<TimeSeriesData.TSMultiVariateDataPoint,
          KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>> {

    @ProcessElement
    public void process(ProcessContext c) {

      TimeSeriesData.TSMultiVariateDataPoint mdp = c.element();

      for (String key : mdp.getDataMap().keySet()) {
        TimeSeriesData.TSDataPoint dp =
            extractDataFromMultiVarientDataPoint(
                c.element(), key, c.element().getDataMap().get(key));
        c.output(KV.of(dp.getKey(), dp));
      }
    }
  }

  public static class ExtractTimeStamp
      extends DoFn<TimeSeriesData.TSMultiVariateDataPoint, TimeSeriesData.TSMultiVariateDataPoint> {

    @ProcessElement
    public void process(ProcessContext c) {

      c.outputWithTimestamp(
          c.element(), new DateTime(Timestamps.toMillis(c.element().getTimestamp())).toInstant());
    }
  }

  private static TimeSeriesData.TSDataPoint extractDataFromMultiVarientDataPoint(
      TimeSeriesData.TSMultiVariateDataPoint meta, String minorKey, TimeSeriesData.Data data) {

    return TimeSeriesData.TSDataPoint.newBuilder()
        .setKey(meta.getKey().toBuilder().setMinorKeyString(minorKey))
        .setTimestamp(meta.getTimestamp())
        .setData(data)
        .putAllMetadata(meta.getMetadataMap())
        .build();
  }
}
