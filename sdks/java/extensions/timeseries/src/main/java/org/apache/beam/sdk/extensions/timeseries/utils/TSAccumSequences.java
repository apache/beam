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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.io.UnsupportedEncodingException;

import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.*;

/** Utility functions for TSAccum. */
@Experimental
public class TSAccumSequences {

  static final Logger LOG = LoggerFactory.getLogger(TSAccumSequences.class);

  private static final String LOWER_WINDOW_BOUNDARY = "LOWER_WINDOW_BOUNDARY";
  private static final String UPPER_WINDOW_BOUNDARY = "UPPER_WINDOW_BOUNDARY";

  private static final String FIRST_TIME_STAMP = "FIRST_TIME_STAMP";
  private static final String LAST_TIME_STAMP = "LAST_TIME_STAMP";

  public static SequenceExample getSequenceExampleFromAccumSequence(
      TimeSeriesData.TSAccumSequence sequence)
      throws UnsupportedEncodingException, InvalidProtocolBufferException {

    SequenceExample.Builder sequenceExample = SequenceExample.newBuilder();

    sequenceExample.setContext(
        Features.newBuilder()
            .putFeature(
                TimeSeriesData.TSKey.KeyType.MAJOR_KEY.name(),
                Feature.newBuilder()
                    .setBytesList(
                        BytesList.newBuilder().addValue(sequence.getKey().getMajorKeyBytes()))
                    .build())
            .putFeature(
                TimeSeriesData.TSKey.KeyType.MINOR_KEY.name(),
                Feature.newBuilder()
                    .setBytesList(
                        BytesList.newBuilder().addValue(sequence.getKey().getMinorKeyStringBytes()))
                    .build())
            .putFeature(
                LOWER_WINDOW_BOUNDARY,
                Feature.newBuilder()
                    .setFloatList(
                        FloatList.newBuilder()
                            .addValue(Timestamps.toMillis(sequence.getLowerWindowBoundary())))
                    .build())
            .putFeature(
                UPPER_WINDOW_BOUNDARY,
                Feature.newBuilder()
                    .setFloatList(
                        FloatList.newBuilder()
                            .addValue(Timestamps.toMillis(sequence.getUpperWindowBoundary())))
                    .build()));

    FeatureList.Builder sum = FeatureList.newBuilder();
    FeatureList.Builder min = FeatureList.newBuilder();
    FeatureList.Builder max = FeatureList.newBuilder();
    FeatureList.Builder first = FeatureList.newBuilder();
    FeatureList.Builder last = FeatureList.newBuilder();

    FeatureList.Builder sumPrev = FeatureList.newBuilder();
    FeatureList.Builder minPrev = FeatureList.newBuilder();
    FeatureList.Builder maxPrev = FeatureList.newBuilder();
    FeatureList.Builder firstPrev = FeatureList.newBuilder();
    FeatureList.Builder lastPrev = FeatureList.newBuilder();

    for (TimeSeriesData.TSAccum accum : sequence.getAccumsList()) {

      sum.addFeature(TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getSum()));
      min.addFeature(TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getMinValue()));
      max.addFeature(TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getMaxValue()));
      first.addFeature(
          TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getFirst().getData()));
      last.addFeature(TSDatas.getFeatureFromTSDataPoint(accum.getDataAccum().getLast().getData()));

      if (accum.getPreviousWindowValue() != null) {
        sumPrev.addFeature(
            TSDatas.getFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getSum()));
        minPrev.addFeature(
            TSDatas.getFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getMinValue()));
        maxPrev.addFeature(
            TSDatas.getFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getMaxValue()));
        firstPrev.addFeature(
            TSDatas.getFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getFirst().getData()));
        lastPrev.addFeature(
            TSDatas.getFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getLast().getData()));
      }
    }

    FeatureLists.Builder features = FeatureLists.newBuilder();
    features.putFeatureList(TimeSeriesData.DownSampleType.SUM.name(), sum.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.MIN.name(), min.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.MAX.name(), max.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.FIRST.name(), first.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.LAST.name(), last.build());

    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.SUM), sumPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.MIN), minPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.MAX), maxPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.FIRST), firstPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.LAST), lastPrev.build());

    sequenceExample.setFeatureLists(features);

    return sequenceExample.build();
  }

  public static String postFixPrevWindowKey(TimeSeriesData.DownSampleType downSampleType) {
    return downSampleType.name() + "_PREV";
  }

  /** Push to tf Examples generated from TSAccum's to BigTable * */
  public static class OutPutToBigTable
      extends PTransform<PCollection<TimeSeriesData.TSAccumSequence>, PCollection<Mutation>> {

    public static final byte[] TF_ACCUM = Bytes.toBytes("TF_ACCUM");

    @Override
    public PCollection<Mutation> expand(PCollection<TimeSeriesData.TSAccumSequence> input) {
      return input.apply(ParDo.of(new WriteTFAccumToBigTable()));
    }

    public static class WriteTFAccumToBigTable
        extends DoFn<TimeSeriesData.TSAccumSequence, Mutation> {

      // Create key structure of majorkey-duration(ms)-lowerWindow-upperWindow
      // Minor key becomes column
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(
            new Put(createBigTableKey(c.element()))
                .addColumn(
                    OutPutToBigTable.TF_ACCUM,
                    Bytes.toBytes(c.element().getKey().getMinorKeyString()),
                    TSAccumSequences.getSequenceExampleFromAccumSequence(c.element())
                        .toByteArray()));
      }
    }

    public static byte[] createBigTableKey(TimeSeriesData.TSAccumSequence accumSequence) {

      return Bytes.toBytes(
          String.join(
              "-",
              accumSequence.getKey().getMajorKey(),
              Long.toString(Durations.toMillis(accumSequence.getDuration())),
              Long.toString(Timestamps.toMillis(accumSequence.getLowerWindowBoundary())),
              Long.toString(Timestamps.toMillis(accumSequence.getUpperWindowBoundary()))));
    }
  }
}
