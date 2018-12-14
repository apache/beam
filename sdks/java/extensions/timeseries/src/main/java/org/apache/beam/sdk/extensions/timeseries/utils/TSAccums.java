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

import static com.google.protobuf.util.Timestamps.add;
import static com.google.protobuf.util.Timestamps.compare;
import static com.google.protobuf.util.Timestamps.toMillis;
import static java.util.Comparator.comparing;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.io.TFRecordIO;
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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.Int64List;

/** Utility functions for TSAccum. */
@Experimental
public class TSAccums {

  static final Logger LOG = LoggerFactory.getLogger(TSAccums.class);

  private static String getSumString(TSAccum accum) {

    return TSDatas.getStringValue(accum.getDataAccum().getSum());
  }

  public static String getMinString(TSAccum accum) {

    return TSDatas.getStringValue(accum.getDataAccum().getMinValue());
  }

  public static String getMaxString(TSAccum accum) {

    return TSDatas.getStringValue(accum.getDataAccum().getMaxValue());
  }

  public static Timestamp getMaxTimeStamp(Timestamp a, Timestamp b) {

    return (Timestamps.comparator().compare(a, b) > 0) ? a : b;
  }

  public static Timestamp getMinTimeStamp(Timestamp a, Timestamp b) {

    if (toMillis(a) == 0 && toMillis(b) > 0) {
      return b;
    }

    if (toMillis(b) == 0) {
      return a;
    }

    return (Timestamps.comparator().compare(a, b) < 0) ? a : b;
  }

  public static TSAccum.Builder merge(TSAccum.Builder accumA, TSAccum accumB) {

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

  public static Example getExampleFromAccum(TSAccum accum) throws UnsupportedEncodingException {
    Example.Builder example = Example.newBuilder();
    return example.setFeatures(getFeaturesFromAccum(accum)).build();
  }

  private static Features getFeaturesFromAccum(TSAccum accum) throws UnsupportedEncodingException {

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
        "LOWER_WINDOW_BOUNDARY",
        Feature.newBuilder()
            .setInt64List(Int64List.newBuilder().addValue(toMillis(accum.getLowerWindowBoundary())))
            .build());

    features.putFeature(
        "UPPER_WINDOW_BOUNDARY",
        Feature.newBuilder()
            .setInt64List(Int64List.newBuilder().addValue(toMillis(accum.getUpperWindowBoundary())))
            .build());

    // Convert Count to Double to make Tensor handling easier with other data types.
    features.putFeature(
        "COUNT",
        TSDatas.tfFeatureFromTSDataPoint(
            TSDatas.createData((double) accum.getDataAccum().getCount().getIntVal())));

    features.putFeature(
        TimeSeriesData.DownSampleType.SUM.name(),
        TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getSum()));
    features.putFeature(
        TimeSeriesData.DownSampleType.MIN.name(),
        TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getMinValue()));
    features.putFeature(
        TimeSeriesData.DownSampleType.MAX.name(),
        TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getMaxValue()));
    features.putFeature(
        TimeSeriesData.DownSampleType.FIRST.name(),
        TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getFirst().getData()));
    features.putFeature(
        TimeSeriesData.DownSampleType.LAST.name(),
        TSDatas.tfFeatureFromTSDataPoint(accum.getDataAccum().getLast().getData()));

    if (accum.getPreviousWindowValue() != null) {
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.SUM),
          TSDatas.tfFeatureFromTSDataPoint(accum.getPreviousWindowValue().getDataAccum().getSum()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.MIN),
          TSDatas.tfFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getMinValue()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.MAX),
          TSDatas.tfFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getMaxValue()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.FIRST),
          TSDatas.tfFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getFirst().getData()));
      features.putFeature(
          postFixPrevWindowKey(TimeSeriesData.DownSampleType.LAST),
          TSDatas.tfFeatureFromTSDataPoint(
              accum.getPreviousWindowValue().getDataAccum().getLast().getData()));
    }

    return features.build();
  }

  public static List<TSAccum> sortByUpperBoundary(List<TSAccum> accums) {
    accums.sort(comparing(tsAccum -> toMillis(tsAccum.getUpperWindowBoundary())));
    return accums;
  }

  /** Push to tf Examples generated from TSAccum's to BigTable. */

  /**
   * BackFill accum sequence, where key started > lower bound of sequence window. Starting from a
   * point != to the start of the seq window, back fill using the first value in window.
   *
   * <p>The first value in the seq window will not have a previous value set.
   *
   * @param accum
   * @param lowerBoundary
   * @param configuration
   * @return
   */
  public static List<TSAccum> generateBackFillHeartBeatValues(
      TSAccum accum, Timestamp lowerBoundary, TSConfiguration configuration) {

    Duration downSample = configuration.downSampleDuration();

    Timestamp upperBoundary = accum.getLowerWindowBoundary();

    List<TSAccum> list = new ArrayList<>();

    TSAccum.Builder first =
        createHBValue(
            accum,
            lowerBoundary,
            add(lowerBoundary, Durations.fromMillis(downSample.getMillis())),
            configuration.fillOption());

    // Remove the previous window value from the first item in the seq as the key may not have even
    // existed in the previous window. This is a compromise.

    first.clearPreviousWindowValue();

    list.add(first.build());

    TSAccum.Builder current = first;

    while (compare(current.getUpperWindowBoundary(), upperBoundary) < 0) {

      current =
          createHBValue(
              current.build(),
              current.getUpperWindowBoundary(),
              add(current.getUpperWindowBoundary(), Durations.fromMillis(downSample.getMillis())),
              configuration.fillOption());

      list.add(current.build());
    }

    return list;
  }

  /**
   * BackFill accum sequence, where key started > lower bound of sequence window. Starting from a
   * point which is != to the end of the seq window, forward fill using the last value
   *
   * @param accum
   * @param upperBoundary
   * @param configuration
   * @return
   */
  public static List<TSAccum> generateForwardFillHeartBeatValues(
      TSAccum accum, Timestamp upperBoundary, TSConfiguration configuration) {

    Duration downSample = configuration.downSampleDuration();

    List<TSAccum> list = new ArrayList<>();

    TSAccum.Builder current = accum.toBuilder();

    while (compare(current.getUpperWindowBoundary(), upperBoundary) < 0) {

      current =
          createHBValue(
              current.build(),
              current.getUpperWindowBoundary(),
              add(current.getUpperWindowBoundary(), Durations.fromMillis(downSample.getMillis())),
              configuration.fillOption());

      list.add(current.build());
    }

    return list;
  }

  private static TSAccum.Builder createHBValue(
      TSAccum accum, Timestamp lower, Timestamp upper, TSConfiguration.BFillOptions bfOption) {

    TSAccum.Builder builder =
        accum
            .toBuilder()
            .setLowerWindowBoundary(lower)
            .setUpperWindowBoundary(upper)
            .putMetadata(TSConfiguration.HEARTBEAT, "")
            .setPreviousWindowValue(accum.toBuilder().clearPreviousWindowValue());

    if (bfOption != TSConfiguration.BFillOptions.LAST_KNOWN_VALUE) {
      builder.clearDataAccum().clearFirstTimeStamp().clearLastTimeStamp();
    }

    return builder;
  }

  /** Push to tf Examples generated from TSAccum's to BigTable. */
  public static class OutPutToBigTable
      extends PTransform<PCollection<TSAccum>, PCollection<Mutation>> {

    private static final byte[] TF_ACCUM = Bytes.toBytes("TF_ACCUM");
    private static final byte[] DOWNSAMPLE_SIZE_MS = Bytes.toBytes("DOWNSAMPLE_SIZE_MS");

    @Override
    public PCollection<Mutation> expand(PCollection<TSAccum> input) {
      return input.apply(ParDo.of(new WriteTFAccumToBigTable()));
    }

    /** Write to BigTable. */
    public static class WriteTFAccumToBigTable extends DoFn<TSAccum, Mutation> {

      @ProcessElement
      public void processElement(DoFn<TSAccum, Mutation>.ProcessContext c) throws Exception {
        c.output(
            new Put(createBigTableKey(c.element()))
                .addColumn(
                    OutPutToBigTable.TF_ACCUM,
                    OutPutToBigTable.DOWNSAMPLE_SIZE_MS,
                    TSAccums.getExampleFromAccum(c.element()).toByteArray()));
      }
    }

    private static byte[] createBigTableKey(TSAccum accum) {
      return Bytes.toBytes(
          String.join(
              "-",
              accum.getKey().getMajorKey(),
              Long.toString(Durations.toMillis(accum.getDuration())),
              Long.toString(toMillis(accum.getLowerWindowBoundary())),
              Long.toString(toMillis(accum.getUpperWindowBoundary()))));
    }
  }

  /** Push to tf Examples generated from TSAccum's to BigTable. */
  public static class ConvertTSAccumToBQTableRow
      extends PTransform<PCollection<TSAccum>, PCollection<TableRow>> {

    private static final byte[] TF_ACCUM = Bytes.toBytes("TF_ACCUM");
    private static final byte[] DOWNSAMPLE_SIZE_MS = Bytes.toBytes("DOWNSAMPLE_SIZE_MS");

    @Override public PCollection<TableRow> expand(PCollection<TSAccum> input) {
      return input.apply(ParDo.of(new WriteTFAccumToBigQuery()));
    }

    public static final String TSACCUM_SCHEMA_MAJOR_KEY = "MAJOR_KEY";
    public static final String TSACCUM_SCHEMA_MINOR_KEY = "MINOR_KEY";
    public static final String TSACCUM_SCHEMA_LOWER_WINDOW_BOUNDARY = "L_WIN_BOUNDARY";
    public static final String TSACCUM_SCHEMA_UPPER_WINDOW_BOUNDARY = "U_WIN_BOUNDARY";
    public static final String TSACCUM_SCHEMA_DURATION = "WIN_DURATION";
    public static final String TSACCUM_SCHEMA_DATA = "DATA";
    public static final String TSACCUM_SCHEMA_DATA_PREVIOUS = "PREVIOUS";
    public static final String TSACCUM_HB = "HB";

    public static final String TSACCUM_DATA_COUNT = "COUNT";
    public static final String TSACCUM_DATA_SUM = "SUM";
    public static final String TSACCUM_DATA_FIRST_TIMESTAMP = "FIRST_TIMESTAMP";
    public static final String TSACCUM_DATA_FIRST_VALUE = "FIRST_VALUE";
    public static final String TSACCUM_DATA_LAST_TIMESTAMP = "LAST_TIMESTAMP";
    public static final String TSACCUM_DATA_LAST_VALUE = "LAST_VALUE";
    public static final String TSACCUM_DATA_MIN = "MIN";
    public static final String TSACCUM_DATA_MAX = "MAX";

    /** Write to BigQuery. */
    public static class WriteTFAccumToBigQuery extends DoFn<TSAccum, TableRow> {

      @ProcessElement public void processElement(DoFn<TSAccum, TableRow>.ProcessContext c)
          throws Exception {

        TSAccum accum = c.element();
        TableRow row = new TableRow();

        // Set all Metadata for the TSAccum
        row.set(TSACCUM_SCHEMA_MAJOR_KEY, accum.getKey().getMajorKey());
        row.set(TSACCUM_SCHEMA_MINOR_KEY, accum.getKey().getMinorKeyString());
        row.set(TSACCUM_SCHEMA_LOWER_WINDOW_BOUNDARY, toMillis(accum.getLowerWindowBoundary())/1000);
        row.set(TSACCUM_SCHEMA_UPPER_WINDOW_BOUNDARY, toMillis(accum.getUpperWindowBoundary())/1000);
        row.set(TSACCUM_SCHEMA_DURATION, Durations.toMillis(accum.getDuration()));
        row.set(TSACCUM_HB, accum.getMetadataMap().containsKey(TSConfiguration.HEARTBEAT));

        // Set a repeated field for all the data points in the accum


        TableRow dataRecord = new TableRow();

        dataRecord.set(TSACCUM_DATA_COUNT, accum.getDataAccum().getCount().getIntVal());
        dataRecord.set(TSACCUM_DATA_SUM, accum.getDataAccum().getSum().getDoubleVal());
        dataRecord.set(TSACCUM_DATA_MAX, accum.getDataAccum().getMaxValue().getDoubleVal());
        dataRecord.set(TSACCUM_DATA_MIN, accum.getDataAccum().getMinValue().getDoubleVal());

        dataRecord.set(TSACCUM_DATA_FIRST_TIMESTAMP, toMillis(accum.getDataAccum().getFirst().getTimestamp())/1000);
        dataRecord.set(TSACCUM_DATA_LAST_TIMESTAMP, toMillis(accum.getDataAccum().getLast().getTimestamp())/1000);

        dataRecord.set(TSACCUM_DATA_FIRST_VALUE, accum.getDataAccum().getFirst().getData().getDoubleVal());
        dataRecord.set(TSACCUM_DATA_LAST_VALUE, accum.getDataAccum().getLast().getData().getDoubleVal());


        row.set(TSACCUM_SCHEMA_DATA, dataRecord);

        if(accum.hasPreviousWindowValue()) {

          TableRow dataRecordPrevious = new TableRow();

          dataRecordPrevious.set(TSACCUM_DATA_COUNT, accum.getPreviousWindowValue().getDataAccum().getCount().getIntVal());
          dataRecordPrevious.set(TSACCUM_DATA_SUM, accum.getPreviousWindowValue().getDataAccum().getSum().getDoubleVal());
          dataRecordPrevious.set(TSACCUM_DATA_MAX, accum.getPreviousWindowValue().getDataAccum().getMaxValue().getDoubleVal());
          dataRecordPrevious.set(TSACCUM_DATA_MIN, accum.getPreviousWindowValue().getDataAccum().getMinValue().getDoubleVal());

          dataRecordPrevious.set(TSACCUM_DATA_FIRST_TIMESTAMP,
              toMillis(accum.getPreviousWindowValue().getDataAccum().getFirst().getTimestamp()) / 1000);
          dataRecordPrevious.set(TSACCUM_DATA_LAST_TIMESTAMP,
              toMillis(accum.getPreviousWindowValue().getDataAccum().getLast().getTimestamp()) / 1000);

          dataRecordPrevious.set(TSACCUM_DATA_FIRST_VALUE,
              accum.getPreviousWindowValue().getDataAccum().getFirst().getData().getDoubleVal());
          dataRecordPrevious.set(TSACCUM_DATA_LAST_VALUE, accum.getPreviousWindowValue().getDataAccum().getLast().getData().getDoubleVal());

          row.set(TSACCUM_SCHEMA_DATA_PREVIOUS, dataRecordPrevious);
        }
        c.output(row);

      }
    }

    public static final TableSchema TS_ACCUM_SCHEMA = new TableSchema().setFields(ImmutableList
        .of((new TableFieldSchema().setName(TSACCUM_SCHEMA_MAJOR_KEY).setType("STRING")),
            (new TableFieldSchema().setName(TSACCUM_SCHEMA_MINOR_KEY).setType("STRING")),
            (new TableFieldSchema().setName(TSACCUM_SCHEMA_LOWER_WINDOW_BOUNDARY).setType("TIMESTAMP")),
            (new TableFieldSchema().setName(TSACCUM_SCHEMA_UPPER_WINDOW_BOUNDARY).setType("TIMESTAMP")),
            (new TableFieldSchema().setName(TSACCUM_SCHEMA_DURATION).setType("FLOAT")),
            (new TableFieldSchema().setName(TSACCUM_HB).setType("BOOLEAN")),
            (new TableFieldSchema().setName(TSACCUM_SCHEMA_DATA).setType("RECORD")
                .setFields(ImmutableList.of(
                  new TableFieldSchema().setName(TSACCUM_DATA_COUNT).setType("FLOAT"),
                  new TableFieldSchema().setName(TSACCUM_DATA_SUM).setType("FLOAT"),
                  new TableFieldSchema().setName(TSACCUM_DATA_MIN).setType("FLOAT"),
                  new TableFieldSchema().setName(TSACCUM_DATA_MAX).setType("FLOAT"),
                  new TableFieldSchema().setName(TSACCUM_DATA_FIRST_TIMESTAMP).setType("TIMESTAMP"),
                  new TableFieldSchema().setName(TSACCUM_DATA_LAST_TIMESTAMP).setType("TIMESTAMP"),
                  new TableFieldSchema().setName(TSACCUM_DATA_FIRST_VALUE).setType("FLOAT"),
                  new TableFieldSchema().setName(TSACCUM_DATA_LAST_VALUE).setType("FLOAT")

            ))),
                    (new TableFieldSchema().setName(TSACCUM_SCHEMA_DATA_PREVIOUS).setType("RECORD")
                        .setFields(ImmutableList.of(
                            new TableFieldSchema().setName(TSACCUM_DATA_COUNT).setType("FLOAT"),
                            new TableFieldSchema().setName(TSACCUM_DATA_SUM).setType("FLOAT"),
                            new TableFieldSchema().setName(TSACCUM_DATA_MIN).setType("FLOAT"),
                            new TableFieldSchema().setName(TSACCUM_DATA_MAX).setType("FLOAT"),
                            new TableFieldSchema().setName(TSACCUM_DATA_FIRST_TIMESTAMP).setType("TIMESTAMP"),
                            new TableFieldSchema().setName(TSACCUM_DATA_LAST_TIMESTAMP).setType("TIMESTAMP"),
                            new TableFieldSchema().setName(TSACCUM_DATA_FIRST_VALUE).setType("FLOAT"),
                            new TableFieldSchema().setName(TSACCUM_DATA_LAST_VALUE).setType("FLOAT")

                        )))
                ));
  }

    /** Push to tf Examples generated from TSAccum's to BigTable. */
  public static class OutputAccumWithTimestamp
      extends PTransform<PCollection<TSAccum>, PCollection<TSAccum>> {

    @Override
    public PCollection<TSAccum> expand(PCollection<TSAccum> input) {
      return input.apply(ParDo.of(new ExtractTimestamp()));
    }

    /** Extract timestamps. */
    public static class ExtractTimestamp extends DoFn<TSAccum, TSAccum> {

      @ProcessElement
      public void processElement(DoFn<TSAccum, TSAccum>.ProcessContext c) throws Exception {
        c.outputWithTimestamp(
            c.element(), new Instant(toMillis(c.element().getLowerWindowBoundary())));
      }
    }
  }

  /** This utility class stores Raw TSAccum protos into TFRecord container. */
  public static class StoreRawTFAccumInTFRecordContainer
      extends PTransform<PCollection<TSAccum>, PDone> {

    String directoryLocation;

    public StoreRawTFAccumInTFRecordContainer(String directoryLocation) {
      this.directoryLocation = directoryLocation;
    }

    @Override
    public PDone expand(PCollection<TSAccum> input) {

      return input
          .apply(
              ParDo.of(
                  new DoFn<TSAccum, byte[]>() {

                    @ProcessElement
                    public void process(ProcessContext c) {
                      c.output(c.element().toByteArray());
                    }
                  }))
          .apply(TFRecordIO.write().to(directoryLocation));
    }
  }

  /** Assign keys. */
  public static class OutPutTSAccumAsKV extends DoFn<TSAccum, KV<TimeSeriesData.TSKey, TSAccum>> {

    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(c.element().getKey(), c.element()));
    }
  }

  /** Assign timestamped keys. */
  public static class OutPutTSAccumAsKVWithTimeBoundary extends DoFn<TSAccum, KV<String, TSAccum>> {

    @ProcessElement
    public void process(ProcessContext c) {

      TimeSeriesData.TSKey key = c.element().getKey();
      TSAccum accum = c.element();

      c.output(
          KV.of(
              String.join(
                  "-",
                  key.getMajorKey(),
                  key.getMinorKeyString(),
                  Long.toString(toMillis(accum.getLowerWindowBoundary())),
                  Long.toString(toMillis(accum.getUpperWindowBoundary()))),
              accum));
    }
  }

  /** Assign keys with pretty printed timestamps. */
  public static class OutPutTSAccumAsKVWithPrettyTimeBoundary
      extends DoFn<TSAccum, KV<String, TSAccum>> {

    @ProcessElement
    public void process(ProcessContext c) {

      TSAccum accum = c.element();

      c.output(KV.of(getTSAccumKeyWithPrettyTimeBoundary(accum), accum));
    }
  }

  public static String getTSAccumMajorMinorKeyAsString(TSAccum accum) {

    TimeSeriesData.TSKey key = accum.getKey();

    return String.join("-", key.getMajorKey(), key.getMinorKeyString());
  }

  public static String getTSAccumKeyMillsTimeBoundary(TSAccum accum) {

    TimeSeriesData.TSKey key = accum.getKey();

    return String.join(
        "-",
        key.getMajorKey(),
        key.getMinorKeyString(),
        Long.toString(toMillis(accum.getLowerWindowBoundary())),
        Long.toString(toMillis(accum.getUpperWindowBoundary())));
  }

  public static String getTSAccumKeyWithPrettyTimeBoundary(TSAccum accum) {

    TimeSeriesData.TSKey key = accum.getKey();

    return String.join(
        "-",
        key.getMajorKey(),
        key.getMinorKeyString(),
        Timestamps.toString(accum.getLowerWindowBoundary()),
        Timestamps.toString(accum.getUpperWindowBoundary()));
  }

  public static String debugDetectOutputDiffBetweenTwoAccums(TSAccum accum1, TSAccum accum2) {

    StringBuilder sb = new StringBuilder();
    if (!accum1.getKey().toByteString().equals(accum2.getKey().toByteString())) {
      sb.append(
          String.format(
              " Accum 1 had Key %s while Accum 2 has key %s", accum1.getKey(), accum2.getKey()));
    }

    if (!accum1
        .getLowerWindowBoundary()
        .toByteString()
        .equals(accum2.getLowerWindowBoundary().toByteString())) {
      sb.append(
          String.format(
              " Accum 1 had LowerWindowBoundary %s while Accum 2 has LowerWindowBoundary %s",
              accum1.getLowerWindowBoundary(), accum2.getLowerWindowBoundary()));
    }

    if (!accum1
        .getUpperWindowBoundary()
        .toByteString()
        .equals(accum2.getUpperWindowBoundary().toByteString())) {
      sb.append(
          String.format(
              " Accum 1 had UpperWindowBoundary %s while Accum 2 has UpperWindowBoundary %s",
              accum1.getUpperWindowBoundary(), accum2.getUpperWindowBoundary()));
    }

    if (!accum1
        .getPreviousWindowValue()
        .toByteString()
        .equals(accum2.getPreviousWindowValue().toByteString())) {
      sb.append(
          String.format(
              " Accum 1  & Accum 2 had difference in PreviousWindowValue accum 1 %s accum 2 %s ",
              accum1.getPreviousWindowValue(), accum2.getPreviousWindowValue()));
    }

    if (!accum1.getDataAccum().toByteString().equals(accum2.getDataAccum().toByteString())) {
      sb.append(
          String.format(
              " Accum 1 had DataAccum %s while Accum 2 has DataAccum %s",
              accum1.getDataAccum(), accum2.getDataAccum()));
    }

    if (!accum1
        .getFirstTimeStamp()
        .toByteString()
        .equals(accum2.getFirstTimeStamp().toByteString())) {
      sb.append(
          String.format(
              " Accum 1 had FirstTimeStamp %s while Accum 2 has FirstTimeStamp %s",
              accum1.getFirstTimeStamp(), accum2.getFirstTimeStamp()));
    }

    if (!accum1
        .getLastTimeStamp()
        .toByteString()
        .equals(accum2.getLastTimeStamp().toByteString())) {
      sb.append(
          String.format(
              " Accum 1 had LastTimeStamp %s while Accum 2 has LastTimeStamp %s",
              accum1.getLastTimeStamp(), accum2.getLastTimeStamp()));
    }

    return sb.toString();
  }

  /** Create CSV. */
  public static class CreateCsv
      extends PTransform<PCollection<KV<TimeSeriesData.TSKey, TSAccum>>, PDone> {

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
    public PDone expand(PCollection<KV<TimeSeriesData.TSKey, TSAccum>> input) {
      input.apply(
          ParDo.of(
              new DoFn<KV<TimeSeriesData.TSKey, TSAccum>, String>() {

                @ProcessElement
                public void process(ProcessContext c, IntervalWindow w) {

                  TSAccum accum = c.element().getValue();

                  String key = c.element().getKey().getMajorKey();

                  key += "-" + c.element().getKey().getMinorKeyString();

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
