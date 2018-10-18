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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.util.Timestamps;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility functions for MultiVarrientTSAccum. */
@Experimental
public class MultiVariateTSAccums {

  private static final String MAJOR_KEY = "ts_key_major";
  private static final String HB = TSConfiguration.HEARTBEAT;

  private static final String FIRST_IN_WINDOW_DATA_VALUE = "first_in_window_data_value";
  private static final String LAST_IN_WINDOW_DATA_VALUE = "last_in_window_data_value";

  private static final String LOWER_WINDOW_BOUNDARY = "lower_window_boundary";
  private static final String UPPER_WINDOW_BOUNDARY = "upper_window_boundary";

  private static final String CURRENT = "current_window";
  private static final String PREVIOUS = "previous_window";

  private static final String COUNT = "count";
  private static final String SUM = "sum";
  private static final String AVG = "avg";
  private static final String MIN = "min";
  private static final String MAX = "max";

  /** BigQuery IO. */
  // TODO: Convert Accum object to repeated fields rather than columns.
  public static class CreateTableRowDoFn
      extends DoFn<KV<TimeSeriesData.TSKey, TimeSeriesData.MultiVariateTSAccum>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {

      TimeSeriesData.MultiVariateTSAccum multiAccum = c.element().getValue();

      // Flatten Object to rows

      TableRow row = new TableRow();

      for (String prop : multiAccum.getPropertiesMap().keySet()) {

        row.set(MAJOR_KEY, c.element().getKey().getMajorKey());

        generateTableRowFromTSAccum(row, prop, multiAccum.getPropertiesMap().get(prop));
      }
      c.output(row);
    }
  }

  private static String addPrefix(String prefix, String str) {
    return prefix + "_" + str;
  }

  public static TableRow generateTableRowFromTSAccum(
      TableRow row, String prefix, TimeSeriesData.TSAccum accum) {

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    // Set HB
    row.set(HB, (accum.getMetadataMap().containsKey(TSConfiguration.HEARTBEAT)));

    // Set Current
    generateTableRowFromTSDataAccum(row, addPrefix(prefix, CURRENT), accum.getDataAccum());

    // Set Previous
    generateTableRowFromTSDataAccum(
        row, addPrefix(prefix, PREVIOUS), accum.getPreviousWindowValue().getDataAccum());

    // Set First & Last data value
    addTSDataPointFieldsToTableTow(
        row,
        addPrefix(prefix, FIRST_IN_WINDOW_DATA_VALUE),
        accum.getDataAccum().getFirst().getData());
    addTSDataPointFieldsToTableTow(
        row,
        addPrefix(prefix, LAST_IN_WINDOW_DATA_VALUE),
        accum.getDataAccum().getLast().getData());

    // Set Window Boundaries
    row.set(
        addPrefix(prefix, LOWER_WINDOW_BOUNDARY),
        simpleDateFormat.format(
            new DateTime(Timestamps.toMillis(accum.getLowerWindowBoundary())).toDate()));
    row.set(
        addPrefix(prefix, UPPER_WINDOW_BOUNDARY),
        simpleDateFormat.format(
            new DateTime(Timestamps.toMillis(accum.getUpperWindowBoundary())).toDate()));

    return row;
  }

  private static TableRow generateTableRowFromTSDataAccum(
      TableRow row, String prefix, TimeSeriesData.Accum accum) {

    // Set Sum
    addTSDataPointFieldsToTableTow(row, addPrefix(prefix, COUNT), accum.getCount());
    addTSDataPointFieldsToTableTow(row, addPrefix(prefix, SUM), accum.getSum());
    addTSDataPointFieldsToTableTow(row, addPrefix(prefix, MIN), accum.getMinValue());
    addTSDataPointFieldsToTableTow(row, addPrefix(prefix, MAX), accum.getMaxValue());

    //addTSDataPointFieldsToTableTow(row, addPrefix(prefix,AVG), accum.getSum()/accum.getCount());

    return row;
  }

  private static TableRow addTSDataPointFieldsToTableTow(
      TableRow row, String rowName, TimeSeriesData.Data data) {

    switch (data.getDataPointCase()) {
      case INT_VAL:
        {
          row.set(rowName, data.getIntVal());
          return row;
        }
      case LONG_VAL:
        {
          row.set(rowName, data.getLongVal());
          return row;
        }
      case DOUBLE_VAL:
        {
          row.set(rowName, data.getDoubleVal());
          return row;
        }
      default:
        //String rowNameWithSuffix = rowName + UNKNOWN_SUFFIX;
        //row.set(rowNameWithSuffix, data.toString());
        return row;
    }
  }

  /** BigQuery Data types. */
  public enum BQDTTYPE {
    INT64,
    FLOAT64
  }

  public static List<TableFieldSchema> getRawSchema(Map<String, BQDTTYPE> bqDtType) {
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();

    fields.add(new TableFieldSchema().setName(HB).setType("BOOLEAN"));

    fields.add(new TableFieldSchema().setName(MAJOR_KEY).setType("STRING"));

    for (String tsColumn : bqDtType.keySet()) {

      fields.add(
          new TableFieldSchema()
              .setName(addPrefix(tsColumn, LOWER_WINDOW_BOUNDARY))
              .setType("TIMESTAMP"));
      fields.add(
          new TableFieldSchema()
              .setName(addPrefix(tsColumn, UPPER_WINDOW_BOUNDARY))
              .setType("TIMESTAMP"));

      fields.add(
          new TableFieldSchema()
              .setName(addPrefix(tsColumn, FIRST_IN_WINDOW_DATA_VALUE))
              .setType(bqDtType.get(tsColumn).name()));
      fields.add(
          new TableFieldSchema()
              .setName(addPrefix(tsColumn, LAST_IN_WINDOW_DATA_VALUE))
              .setType(bqDtType.get(tsColumn).name()));

      fields.addAll(addDataPoints(addPrefix(tsColumn, CURRENT)));
      fields.addAll(addDataPoints(addPrefix(tsColumn, PREVIOUS)));
    }

    return fields;
  }

  private static List<TableFieldSchema> addDataPoints(String prefix) {

    List<TableFieldSchema> fields = new ArrayList<>();

    // Hard coded for demo
    fields.add(new TableFieldSchema().setName(addPrefix(prefix, COUNT)).setType("INT64"));

    fields.add(new TableFieldSchema().setName(addPrefix(prefix, SUM)).setType("FLOAT64"));
    fields.add(new TableFieldSchema().setName(addPrefix(prefix, AVG)).setType("FLOAT64"));
    fields.add(new TableFieldSchema().setName(addPrefix(prefix, MIN)).setType("FLOAT64"));
    fields.add(new TableFieldSchema().setName(addPrefix(prefix, MAX)).setType("FLOAT64"));

    return fields;
  }

  /** */
  public static class GroupByMajorKey
      extends PTransform<
          PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>,
          PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.MultiVariateTSAccum>>> {

    TSConfiguration configuration;

    public GroupByMajorKey(@Nullable String name, TSConfiguration configuration) {
      super(name);
      this.configuration = configuration;
    }

    private static final Logger LOG = LoggerFactory.getLogger(GroupByMajorKey.class);

    @Override
    public PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.MultiVariateTSAccum>> expand(
        PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> input) {

      return input
          .apply(
              "Re-Key to Major Key",
              ParDo.of(
                  new DoFn<
                      KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>,
                      KV<String, TimeSeriesData.TSAccum>>() {

                    @ProcessElement
                    public void process(ProcessContext c) {

                      c.outputWithTimestamp(
                          KV.of(
                              c.element().getKey().getMajorKey()
                                  + Timestamps.toMillis(
                                      c.element().getValue().getUpperWindowBoundary()),
                              c.element().getValue()),
                          new Instant(
                              Timestamps.toMillis(
                                  c.element().getValue().getUpperWindowBoundary())));
                    }
                  }))
          .apply(GroupByKey.create())
          .apply(
              ParDo.of(
                  new DoFn<
                      KV<String, Iterable<TimeSeriesData.TSAccum>>,
                      KV<TimeSeriesData.TSKey, TimeSeriesData.MultiVariateTSAccum>>() {

                    @ProcessElement
                    public void process(ProcessContext c) {

                      // TODO : Fix this mess...
                      TimeSeriesData.TSKey key = null;

                      TimeSeriesData.MultiVariateTSAccum.Builder multiVariateTSAccum =
                          TimeSeriesData.MultiVariateTSAccum.newBuilder();

                      for (TimeSeriesData.TSAccum accum : c.element().getValue()) {
                        multiVariateTSAccum.putProperties(
                            accum.getKey().getMinorKeyString(), accum);
                        key = accum.getKey();
                      }
                      c.output(KV.of(key, multiVariateTSAccum.build()));
                    }
                  }));
    }
  }
}
