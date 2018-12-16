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

import static com.google.protobuf.util.Timestamps.compare;

import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccumSequence;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Feature;
import org.tensorflow.example.FeatureList;
import org.tensorflow.example.FeatureLists;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.SequenceExample;

/** Utility functions for TSAccum. */
@Experimental
public class TSAccumSequences {

  static final Logger LOG = LoggerFactory.getLogger(TSAccumSequences.class);

  private static final String LOWER_WINDOW_BOUNDARY = "LOWER_WINDOW_BOUNDARY";
  private static final String UPPER_WINDOW_BOUNDARY = "UPPER_WINDOW_BOUNDARY";

  private static final String FIRST_TIME_STAMP = "FIRST_TIME_STAMP";
  private static final String LAST_TIME_STAMP = "LAST_TIME_STAMP";

  public static SequenceExample getSequenceExampleFromAccumSequence(TSAccumSequence sequence)
      throws UnsupportedEncodingException, InvalidProtocolBufferException {

    SequenceExample.Builder sequenceExample = SequenceExample.newBuilder();

    sequenceExample.setContext(
        Features.newBuilder()
            .putFeature(
                TSKey.KeyType.MAJOR_KEY.name(),
                Feature.newBuilder()
                    .setBytesList(
                        BytesList.newBuilder().addValue(sequence.getKey().getMajorKeyBytes()))
                    .build())
            .putFeature(
                TSKey.KeyType.MINOR_KEY.name(),
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

    FeatureList.Builder count = FeatureList.newBuilder();
    FeatureList.Builder sum = FeatureList.newBuilder();
    FeatureList.Builder min = FeatureList.newBuilder();
    FeatureList.Builder max = FeatureList.newBuilder();
    FeatureList.Builder first = FeatureList.newBuilder();
    FeatureList.Builder last = FeatureList.newBuilder();

    FeatureList.Builder countPrev = FeatureList.newBuilder();
    FeatureList.Builder sumPrev = FeatureList.newBuilder();
    FeatureList.Builder minPrev = FeatureList.newBuilder();
    FeatureList.Builder maxPrev = FeatureList.newBuilder();
    FeatureList.Builder firstPrev = FeatureList.newBuilder();
    FeatureList.Builder lastPrev = FeatureList.newBuilder();

    for (TimeSeriesData.TSAccum accum : sequence.getAccumsList()) {

      count.addFeature(
          TSDatas.tfFeatureFromTSDataPoint(
              TSDatas.createData((double) accum.getDataAccum().getCount().getIntVal())));

      sum.addFeature(TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getSum()));
      min.addFeature(TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getMinValue()));
      max.addFeature(TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getMaxValue()));
      first.addFeature(TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getFirst().getData()));
      last.addFeature(TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getLast().getData()));

      if (accum.hasPreviousWindowValue()) {
        countPrev.addFeature(
            TSDatas.tfFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getCount()));

        sumPrev.addFeature(
            TSDatas.tfFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getSum()));

        minPrev.addFeature(
            TSDatas.tfFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getMinValue()));

        maxPrev.addFeature(
            TSDatas.tfFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getMaxValue()));

        firstPrev.addFeature(
            TSDatas.tfFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getFirst().getData()));

        lastPrev.addFeature(
            TSDatas.tfFeatureFromTSDataPoint(
                accum.getPreviousWindowValue().getDataAccum().getLast().getData()));
      }
    }

    FeatureLists.Builder features = FeatureLists.newBuilder();
    features.putFeatureList("COUNT", count.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.SUM.name(), sum.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.MIN.name(), min.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.MAX.name(), max.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.FIRST.name(), first.build());
    features.putFeatureList(TimeSeriesData.DownSampleType.LAST.name(), last.build());

    features.putFeatureList(postFixPrevWindowKey("COUNT"), countPrev.build());

    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.SUM.name()), sumPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.MIN.name()), minPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.MAX.name()), maxPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.FIRST.name()), firstPrev.build());
    features.putFeatureList(
        postFixPrevWindowKey(TimeSeriesData.DownSampleType.LAST.name()), lastPrev.build());

    sequenceExample.setFeatureLists(features);

    return sequenceExample.build();
  }

  public static String postFixPrevWindowKey(String str) {
    return str + "_PREV";
  }

  /** Push to tf Examples generated from TSAccum's to BigTable. */
  public static class OutPutToBigTable
      extends PTransform<PCollection<TSAccumSequence>, PCollection<Mutation>> {

    private static final byte[] TF_ACCUM = Bytes.toBytes("TF_ACCUM");

    @Override
    public PCollection<Mutation> expand(PCollection<TSAccumSequence> input) {
      return input.apply(ParDo.of(new WriteTFAccumToBigTable()));
    }

    /** Write to BigTable. */
    public static class WriteTFAccumToBigTable extends DoFn<TSAccumSequence, Mutation> {

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

    public static byte[] createBigTableKey(TSAccumSequence accumSequence) {

      return Bytes.toBytes(
          String.join(
              "-",
              accumSequence.getKey().getMajorKey(),
              Long.toString(Durations.toMillis(accumSequence.getDuration())),
              Long.toString(Timestamps.toMillis(accumSequence.getLowerWindowBoundary())),
              Long.toString(Timestamps.toMillis(accumSequence.getUpperWindowBoundary()))));
    }
  }

  public static String getTSAccumSequenceKeyMillsTimeBoundary(TSAccumSequence accumSequence) {

    TimeSeriesData.TSKey key = accumSequence.getKey();

    return String.join(
        "-",
        key.getMajorKey(),
        key.getMinorKeyString(),
        Long.toString(Timestamps.toMillis(accumSequence.getLowerWindowBoundary())),
        Long.toString(Timestamps.toMillis(accumSequence.getUpperWindowBoundary())));
  }

  public static String getTSAccumSequenceKeyWithPrettyTimeBoundary(TSAccumSequence accumSequence) {

    TimeSeriesData.TSKey key = accumSequence.getKey();

    return String.join(
        "-",
        key.getMajorKey(),
        key.getMinorKeyString(),
        Timestamps.toString(accumSequence.getLowerWindowBoundary()),
        Timestamps.toString(accumSequence.getUpperWindowBoundary()));
  }

  public static boolean checkIfSequenceHasGaps(TSAccumSequence seq, TSConfiguration configuration) {

    Duration duration = seq.getDuration();
    long maxAccums =
        (long)
            Math.floor(
                Durations.toMillis(duration) / configuration.downSampleDuration().getMillis());
    if (maxAccums < seq.getAccumsCount()) {
      return true;
    }
    return false;
  }

  /**
   * Dependent on the configuration different rules can be applied on TSAccumSequences. - Check to
   * see if incomplete sequences should be discarded. true will be returned if the rules allow the
   * seq to be published false will be returned if the rules allow the seq to be published - Check
   * if incomplete sequences should be forward filled. - Check if incomplete sequecnes should be
   * back filled.
   *
   * <p>This method will mutate the List if the rules require it.
   *
   * @param options
   * @param fixedWindowDuration
   * @param list
   * @param lowerFixedWindowBoundary
   * @param upperFixedWindowBoundary
   * @return true or false dependent on if the sequence should be published
   */
  public static boolean applySequenceOuputRules(
      TSConfiguration options,
      org.joda.time.Duration fixedWindowDuration,
      List<TimeSeriesData.TSAccum> list,
      Timestamp lowerFixedWindowBoundary,
      Timestamp upperFixedWindowBoundary) {

    if (list.size() > 0) {

      TSAccums.sortByUpperBoundary(list);

      if (LOG.isInfoEnabled()) {
        StringBuilder sb = new StringBuilder();
        for (TimeSeriesData.TSAccum accum : list) {
          sb.append(
              new Instant().withMillis(Timestamps.toMillis(accum.getLowerWindowBoundary()))
                  + "  :  ");
        }
        LOG.debug(sb.toString());
      }

      // If check discard incomplete seq is enabled and there is a leading or training gap then
      // discard sequence.

      if (options.discardIncompleteSequences()
          && list.size() != calculateExpectedNumberOfElements(options, fixedWindowDuration)) {
        return false;
      }

      TimeSeriesData.TSAccum firstAccumInList = list.get(0);
      TimeSeriesData.TSAccum lastAccumInList = list.get(list.size() - 1);

      if (!firstAccumInList.hasLowerWindowBoundary()
          || !firstAccumInList.hasUpperWindowBoundary()) {
        throw new IllegalStateException(
            String.format("Accum %s is missing lower or upper window boundary.", firstAccumInList));
      }

      // The output from OrderOutput sets the TSAccum timestamp == the upper window boundary of the accum
      if (Timestamps.compare(firstAccumInList.getUpperWindowBoundary(), lowerFixedWindowBoundary)
          < 0) {
        throw new IllegalStateException(
            String.format(
                "Accum had lower window boundary %s than fixed window lower boundary %s",
                firstAccumInList.getLowerWindowBoundary(), lowerFixedWindowBoundary));
      }

      // We should not have a data point where its timestamp + fixedDuration is >
      // end of the current Window.

      if (!lastAccumInList.hasLowerWindowBoundary() || !lastAccumInList.hasUpperWindowBoundary()) {
        throw new IllegalStateException(
            String.format("Accum %s is missing lower or upper window boundary.", lastAccumInList));
      }

      if (Timestamps.compare(lastAccumInList.getUpperWindowBoundary(), upperFixedWindowBoundary)
          > 0) {
        throw new IllegalStateException(
            String.format(
                "Accum had higher window boundary %s than fixed window upper boundary %s",
                firstAccumInList.getUpperWindowBoundary(), upperFixedWindowBoundary));
      }

      // Backfill the sequence if requested
      if (options.backPadSequencedOutput()
          && compare(firstAccumInList.getLowerWindowBoundary(), lowerFixedWindowBoundary) != 0) {

        LOG.debug(
            "Backfilling Sequence with lower boundary {} and upper boundary {} first accum in list  is {} ",
            Timestamps.toString(lowerFixedWindowBoundary),
            Timestamps.toString(upperFixedWindowBoundary),
            TSAccums.getTSAccumKeyWithPrettyTimeBoundary(firstAccumInList));

        list.set(
            0,
            checkAndUpdateIfFirstValueInSeqHasNotPreviousWindowValue(
                firstAccumInList, options.downSampleDuration()));

        list.addAll(
            TSAccums.generateBackFillHeartBeatValues(
                firstAccumInList, lowerFixedWindowBoundary, options));
      }

      // Forward fill the sequence if requested
      if (options.forwardPadSequencedOutput()
          && compare(lastAccumInList.getUpperWindowBoundary(), upperFixedWindowBoundary) != 0) {

        LOG.debug(
            "Forward Sequence with lower boundary {} and upper boundary {} last accum in list  is {} ",
            Timestamps.toString(lowerFixedWindowBoundary),
            Timestamps.toString(upperFixedWindowBoundary),
            TSAccums.getTSAccumKeyWithPrettyTimeBoundary(lastAccumInList));

        list.addAll(
            TSAccums.generateForwardFillHeartBeatValues(
                lastAccumInList, upperFixedWindowBoundary, options));
      }
    }

    TSAccums.sortByUpperBoundary(list);
    return true;
  }

  // Calculate the list size based on the Downsample configuration and the fixedWindowDuration

  private static long calculateExpectedNumberOfElements(
      TSConfiguration configuration, org.joda.time.Duration fixedWindowDuration) {

    return fixedWindowDuration.getMillis() / configuration.downSampleDuration().getMillis();
  }

  // Check if the first has not got a previousWindow value set, if not then set it

  private static TimeSeriesData.TSAccum checkAndUpdateIfFirstValueInSeqHasNotPreviousWindowValue(
      TimeSeriesData.TSAccum accum, org.joda.time.Duration downSampleDuration) {
    if (accum.hasPreviousWindowValue()) {
      return accum;
    }

    return accum
        .toBuilder()
        .setPreviousWindowValue(
            accum
                .toBuilder()
                .setLowerWindowBoundary(
                    Timestamps.subtract(
                        accum.getLowerWindowBoundary(),
                        Durations.fromMillis(downSampleDuration.getMillis())))
                .setUpperWindowBoundary(accum.getLowerWindowBoundary())
                .putMetadata(TSConfiguration.HEARTBEAT, ""))
        .build();
  }
}
