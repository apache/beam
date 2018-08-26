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

package org.apache.beam.examples.timeseries.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.*;

/** Utility functions for TSAccum. */
@Experimental
public class TSAccums {

  static final Logger LOG = LoggerFactory.getLogger(TSAccums.class);

  public static String getSumString(TimeSeriesData.TSAccum accum) {

    return TSDatas.getStringValue(accum.getDataAccum().getSum());
  }

  public static String getMinString(TimeSeriesData.TSAccum accum) {

    return TSDatas.getStringValue(accum.getDataAccum().getMinValue());
  }

  public static String getMaxString(TimeSeriesData.TSAccum accum) {

    return TSDatas.getStringValue(accum.getDataAccum().getMaxValue());
  }

  public static Timestamp getMaxTimeStamp(Timestamp a, Timestamp b) {

    return (Timestamps.comparator().compare(a, b) > 0) ? a : b;
  }

  public static Timestamp getMinTimeStamp(Timestamp a, Timestamp b) {

    if (Timestamps.toMillis(a) == 0 && Timestamps.toMillis(b) > 0) {
      return b;
    }

    if (Timestamps.toMillis(b) == 0) {
      return a;
    }

    return (Timestamps.comparator().compare(a, b) < 0) ? a : b;
  }

  public static TimeSeriesData.TSAccum.Builder merge(
      TimeSeriesData.TSAccum.Builder accumA, TimeSeriesData.TSAccum accumB) {

    if (!accumA.hasKey()) {
      accumA.setKey(accumB.getKey());
    }
    accumA.setDataAccum(mergeDataAccum(accumA.getDataAccum(), accumB.getDataAccum()));
    accumA.setLastTimeStamp(
        TSAccums.getMaxTimeStamp(accumA.getLastTimeStamp(), accumB.getLastTimeStamp()));
    accumA.setFirstTimeStamp(
        TSAccums.getMinTimeStamp(accumA.getFirstTimeStamp(), accumB.getFirstTimeStamp()));

    return accumA;
  }

  public static TimeSeriesData.Accum mergeDataAccum(
      TimeSeriesData.Accum a, TimeSeriesData.Accum b) {

    TimeSeriesData.Accum.Builder data = TimeSeriesData.Accum.newBuilder();

    data.setFirst(TSDatas.findMinTimeStamp(a.getFirst(), b.getFirst()));
    data.setLast(TSDatas.findMaxTimeStamp(a.getLast(), b.getLast()));

    data.setCount(
        TimeSeriesData.Data.newBuilder()
            .setIntVal(a.getCount().getIntVal() + b.getCount().getIntVal()));

    data.setSum(TSDatas.sumData(a.getSum(), b.getSum()));
    data.setMinValue(TSDatas.findMinDataIfSet(a.getMinValue(), b.getMinValue()));
    data.setMaxValue(TSDatas.findMaxData(a.getMaxValue(), b.getMaxValue()));

    return data.build();
  }

  public static String postFixPrevWindowKey(TimeSeriesData.DownSampleType downSampleType) {
    return downSampleType.name() + "_PREV";
  }

  public static Example getExampleFromAccum(TimeSeriesData.TSAccum accum)
      throws UnsupportedEncodingException {
    Example.Builder example = Example.newBuilder();
    return example.setFeatures(getFeaturesFromAccum(accum)).build();
  }

  private static Features getFeaturesFromAccum(TimeSeriesData.TSAccum accum)
      throws UnsupportedEncodingException {

    Features.Builder features = Features.newBuilder();

    features.putFeature(
        TimeSeriesData.TSKey.KeyType.MAJOR_KEY.name(),
        Feature.newBuilder()
            .setBytesList(
                BytesList.newBuilder()
                    .addValue(ByteString.copyFrom(accum.getKey().getMajorKey(), "UTF-8")))
            .build());

    features.putFeature(
        TimeSeriesData.TSKey.KeyType.MINOR_KEY.name(),
        Feature.newBuilder()
            .setBytesList(
                BytesList.newBuilder()
                    .addValue(ByteString.copyFrom(accum.getKey().getMinorKeyString(), "UTF-8")))
            .build());

    features.putFeature(
        TimeSeriesData.DownSampleType.SUM.name(),
        TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getSum()));
    features.putFeature(
        TimeSeriesData.DownSampleType.MIN.name(),
        TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getMinValue()));
    features.putFeature(
        TimeSeriesData.DownSampleType.MAX.name(),
        TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getMaxValue()));
    features.putFeature(
        TimeSeriesData.DownSampleType.FIRST.name(),
        TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getFirst().getData()));
    features.putFeature(
        TimeSeriesData.DownSampleType.LAST.name(),
        TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getLast().getData()));

    if (accum.getPreviousWindowValue() != null) {
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.SUM),
          TSDatas.getFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getSum()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.MIN),
          TSDatas.getFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getMinValue()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.MAX),
          TSDatas.getFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getMaxValue()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.FIRST),
          TSDatas.getFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getFirst().getData()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.LAST),
          TSDatas.getFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getLast().getData()));
    }

    return features.build();
  }

  public static List<TimeSeriesData.TSAccum> sortAccumList(List<TimeSeriesData.TSAccum> accums) {

    accums.sort(
        new Comparator<TimeSeriesData.TSAccum>() {

          @Override
          public int compare(TimeSeriesData.TSAccum o1, TimeSeriesData.TSAccum o2) {
            if (Timestamps.toMillis(o1.getUpperWindowBoundary())
                > Timestamps.toMillis(o2.getUpperWindowBoundary())) {
              return 1;
            }
            if (Timestamps.toMillis(o1.getUpperWindowBoundary())
                < Timestamps.toMillis(o2.getUpperWindowBoundary())) {
              return -1;
            }
            return 0;
          }
        });

    return accums;
  }

  /** Push to TF Examples generated from TSAccum's to BigTable */
  public static class OutPutToBigTable
      extends PTransform<PCollection<TimeSeriesData.TSAccum>, PCollection<Mutation>> {

    public static final byte[] TF_ACCUM = Bytes.toBytes("TF_ACCUM");
    public static final byte[] DOWNSAMPLE_SIZE_MS = Bytes.toBytes("DOWNSAMPLE_SIZE_MS");

    @Override
    public PCollection<Mutation> expand(PCollection<TimeSeriesData.TSAccum> input) {
      return input.apply(ParDo.of(new WriteTFAccumToBigTable()));
    }

    public static class WriteTFAccumToBigTable extends DoFn<TimeSeriesData.TSAccum, Mutation> {

      @ProcessElement
      public void processElement(DoFn<TimeSeriesData.TSAccum, Mutation>.ProcessContext c)
          throws Exception {
        c.output(
            new Put(createBigTableKey(c.element()))
                .addColumn(
                    OutPutToBigTable.TF_ACCUM,
                    OutPutToBigTable.DOWNSAMPLE_SIZE_MS,
                    TSAccums.getExampleFromAccum(c.element()).toByteArray()));
      }
    }

    public static byte[] createBigTableKey(TimeSeriesData.TSAccum accum) {
      return Bytes.toBytes(
          String.join(
              "-",
              accum.getKey().getMajorKey(),
              Long.toString(Durations.toMillis(accum.getDuration())),
              Long.toString(Timestamps.toMillis(accum.getLowerWindowBoundary())),
              Long.toString(Timestamps.toMillis(accum.getUpperWindowBoundary()))));
    }
  }

  public static class CreateCsv
      extends PTransform<PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>, PDone> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateCsv.class);

    String destination;
    boolean logOutput = false;

    public CreateCsv(@Nullable String name, String destination, boolean logOutput) {
      super(name);
      this.destination = destination;
      this.logOutput = logOutput;
    }

    public CreateCsv(String destination, boolean logOutput) {
      this.destination = destination;
      this.logOutput = logOutput;
    }

    @Override
    public PDone expand(PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {
      input.apply(
          ParDo.of(
              new DoFn<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>, String>() {

                @ProcessElement
                public void process(ProcessContext c, IntervalWindow w) {

                  TimeSeriesData.TSAccum accum = c.element().getValue();

                  String key = c.element().getKey().getMajorKey();

                  key += "-" + (c.element().getKey().getMinorKeyString());

                  String csvLine =
                      String.join(
                          ",",
                          key,
                          w.start().toString(),
                          w.end().toString(),
                          "First TS" + Timestamps.toString(accum.getFirstTimeStamp()),
                          "Last TS" + Timestamps.toString(accum.getLastTimeStamp()),
                          TSAccums.getMinString(accum),
                          TSAccums.getMaxString(accum),
                          TSAccums.getSumString(accum));

                  if (logOutput) {
                    LOG.info(csvLine);
                  }

                  c.output(csvLine);
                }
              }));

      return PDone.in(input.getPipeline());
    }
  }
}
