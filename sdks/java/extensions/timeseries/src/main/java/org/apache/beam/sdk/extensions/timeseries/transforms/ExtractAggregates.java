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

import java.util.Iterator;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.TimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a Univariate timeseries this transform will downsample and aggregate the values based on
 * the TSConfigration.
 */
@SuppressWarnings("serial")
@Experimental
public class ExtractAggregates
    extends PTransform<
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>>,
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExtractAggregates.class);

  @Override
  public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> expand(
      PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>> input) {

    TSConfiguration options =
        TSConfiguration.createConfigurationFromOptions(
            input.getPipeline().getOptions().as(TimeSeriesOptions.class));

    return input
        .apply(Window.into(FixedWindows.of(options.downSampleDuration())))
        .apply(Combine.perKey(new DownSampleCombinerGen()))
        .apply(new GetWindowData());
  }

  /** Creates a down sampled accumulator for all data types. */
  public static class DownSampleCombinerGen
      extends CombineFn<
          TimeSeriesData.TSDataPoint, TimeSeriesData.TSAccum, TimeSeriesData.TSAccum> {

    @Override
    public TimeSeriesData.TSAccum createAccumulator() {
      return TimeSeriesData.TSAccum.newBuilder().build();
    }

    @Override
    public TimeSeriesData.TSAccum addInput(
        TimeSeriesData.TSAccum accumulator, TimeSeriesData.TSDataPoint dataPoint) {

      TimeSeriesData.TSAccum.Builder accumBuilder = TimeSeriesData.TSAccum.newBuilder(accumulator);

      accumBuilder.setKey(dataPoint.getKey());

      // Update the last timestamp value seen in this accum
      accumBuilder.setLastTimeStamp(
          TSAccums.getMaxTimeStamp(accumBuilder.getLastTimeStamp(), dataPoint.getTimestamp()));

      // Update the first timestamp value seen in this accum
      accumBuilder.setFirstTimeStamp(
          TSAccums.getMinTimeStamp(accumBuilder.getFirstTimeStamp(), dataPoint.getTimestamp()));

      TimeSeriesData.Data newData = dataPoint.getData();

      TimeSeriesData.Accum.Builder dataAccum = TimeSeriesData.Accum.newBuilder();

      dataAccum.setCount(
          TimeSeriesData.Data.newBuilder()
              .setIntVal(accumulator.getDataAccum().getCount().getIntVal() + 1));

      dataAccum.setSum(TSDatas.sumData(accumulator.getDataAccum().getSum(), newData));

      dataAccum.setMinValue(
          TSDatas.findMinDataIfSet(accumulator.getDataAccum().getMinValue(), newData));

      dataAccum.setMaxValue(TSDatas.findMaxData(accumulator.getDataAccum().getMaxValue(), newData));

      // Keep the first data point seen in this accum without mutation
      dataAccum.setFirst(
          TSDatas.findMinTimeStamp(accumulator.getDataAccum().getFirst(), dataPoint));

      // Keep the last data point seen in this accum without mutation
      dataAccum.setLast(TSDatas.findMaxTimeStamp(accumulator.getDataAccum().getLast(), dataPoint));

      accumBuilder.setDataAccum(dataAccum);

      LOG.debug("Added input " + dataPoint);

      return accumBuilder.build();
    }

    @Override
    public TimeSeriesData.TSAccum mergeAccumulators(Iterable<TimeSeriesData.TSAccum> accumulators) {

      Iterator<TimeSeriesData.TSAccum> iterator = accumulators.iterator();

      TimeSeriesData.TSAccum.Builder current = TimeSeriesData.TSAccum.newBuilder(iterator.next());

      while (iterator.hasNext()) {
        TimeSeriesData.TSAccum next = iterator.next();
        TSAccums.merge(current, next);
      }

      return current.build();
    }

    @Override
    public TimeSeriesData.TSAccum extractOutput(TimeSeriesData.TSAccum accum) {

      LOG.debug("Output of Agg is : " + accum);

      return accum;
    }
  }
}
