/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;

/**
 * A streaming Dataflow Example using BigQuery output, in the 'traffic sensor' domain.
 *
 * <p>Concepts: The streaming runner, sliding windows, PubSub topic ingestion, use of the AvroCoder
 * to encode a custom class, and custom Combine transforms.
 *
 * <p> This pipeline takes as input traffic sensor data from a PubSub topic, and analyzes it using
 * SlidingWindows. For each window, it finds the lane that had the highest flow recorded, for each
 * sensor station. It writes those max values along with auxiliary info to a BigQuery table.
 *
 * <p> This pipeline expects input from
 * <a href="https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/tree/master/gce-cmdline-publisher">
 * this script</a>,
 * which publishes traffic sensor data to a PubSub topic. After you've started this pipeline, start
 * up the input generation script as per its instructions. The default SlidingWindow parameters
 * assume that you're running this script with the {@literal --replay} flag, which simulates pauses
 * in the sensor data publication.
 *
 * <p> To run this example using the Dataflow service, you must provide an input
 * PubSub topic and an output BigQuery table, using the {@literal --inputTopic},
 * {@literal --dataset}, and {@literal --table} options. Since this is a streaming
 * pipeline that never completes, select the non-blocking pipeline runner by specifying
 * {@literal --runner=DataflowPipelineRunner}.
 *
 * <p> When you are done running the example, cancel your pipeline so that you do not continue to
 * be charged for its instances. You can do this by visiting
 * https://console.developers.google.com/project/your-project-name/dataflow/job-id
 * in the Developers Console. You should also terminate the generator script so that you do not
 * use unnecessary PubSub quota.
 */
public class TrafficStreamingMaxLaneFlow {

  static final int WINDOW_DURATION = 60;  // Default sliding window duration in minutes
  static final int WINDOW_SLIDE_EVERY = 5;  // Default window 'slide every' setting in minutes

  /**
   * This class holds information about each lane in a station reading, along with some general
   * information from the reading.
   */
  @DefaultCoder(AvroCoder.class)
  static class LaneInfo {
    @Nullable String stationId;
    @Nullable String lane;
    @Nullable String direction;
    @Nullable String freeway;
    @Nullable String recordedTimestamp;
    @Nullable Integer laneFlow;
    @Nullable Integer totalFlow;
    @Nullable Double laneAO;
    @Nullable Double laneAS;

    public LaneInfo() {}

    public LaneInfo(String stationId, String lane, String direction, String freeway,
        String timestamp, Integer laneFlow, Double laneAO,
        Double laneAS, Integer totalFlow) {
      this.stationId = stationId;
      this.lane = lane;
      this.direction = direction;
      this.freeway = freeway;
      this.recordedTimestamp = timestamp;
      this.laneFlow = laneFlow;
      this.laneAO = laneAO;
      this.laneAS = laneAS;
      this.totalFlow = totalFlow;
    }

    public String getStationId() {
      return this.stationId;
    }
    public String getLane() {
      return this.lane;
    }
    public String getDirection() {
      return this.direction;
    }
    public String getFreeway() {
      return this.freeway;
    }
    public String getRecordedTimestamp() {
      return this.recordedTimestamp;
    }
    public Integer getLaneFlow() {
      return this.laneFlow;
    }
    public Double getLaneAO() {
      return this.laneAO;
    }
    public Double getLaneAS() {
      return this.laneAS;
    }
    public Integer getTotalFlow() {
      return this.totalFlow;
    }
  }

  /**
   * Extract flow information for each of the 8 lanes in a reading, and output as separate tuples.
   * This will let us determine which lane has the max flow for that station over the span of the
   * window, and output not only the max flow from that calculcation, but other associated
   * information. The number of lanes for which data is present depends upon which freeway the data
   * point comes from.
   */
  static class ExtractFlowInfoFn extends DoFn<String, KV<String, LaneInfo>> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      String[] items = c.element().split(",");
      // extract the sensor information for the lanes from the input string fields.
      String timestamp = items[0];
      String stationId = items[1];
      String freeway = items[2];
      String direction = items[3];
      Integer totalFlow = tryIntParse(items[7]);
      // lane 1
      Integer lane1Flow = tryIntParse(items[11]);
      Double lane1AO = tryDoubleParse(items[12]);
      Double lane1AS = tryDoubleParse(items[13]);
      // lane2
      Integer lane2Flow = tryIntParse(items[16]);
      Double lane2AO = tryDoubleParse(items[17]);
      Double lane2AS = tryDoubleParse(items[18]);
      // lane3
      Integer lane3Flow = tryIntParse(items[21]);
      Double lane3AO = tryDoubleParse(items[22]);
      Double lane3AS = tryDoubleParse(items[23]);
      // lane4
      Integer lane4Flow = tryIntParse(items[26]);
      Double lane4AO = tryDoubleParse(items[27]);
      Double lane4AS = tryDoubleParse(items[28]);
      // lane5
      Integer lane5Flow = tryIntParse(items[31]);
      Double lane5AO = tryDoubleParse(items[32]);
      Double lane5AS = tryDoubleParse(items[33]);
      // lane6
      Integer lane6Flow = tryIntParse(items[36]);
      Double lane6AO = tryDoubleParse(items[37]);
      Double lane6AS = tryDoubleParse(items[38]);
      // lane7
      Integer lane7Flow = tryIntParse(items[41]);
      Double lane7AO = tryDoubleParse(items[42]);
      Double lane7AS = tryDoubleParse(items[43]);
      // lane8
      Integer lane8Flow = tryIntParse(items[46]);
      Double lane8AO = tryDoubleParse(items[47]);
      Double lane8AS = tryDoubleParse(items[48]);

      // For each lane in the reading, output LaneInfo keyed to its station.
      LaneInfo laneInfo1 = new LaneInfo(stationId, "lane1", direction, freeway, timestamp,
          lane1Flow, lane1AO, lane1AS, totalFlow);
      c.output(KV.of(stationId, laneInfo1));
      LaneInfo laneInfo2 = new LaneInfo(stationId, "lane2", direction, freeway, timestamp,
          lane2Flow, lane2AO, lane2AS, totalFlow);
      c.output(KV.of(stationId, laneInfo2));
      LaneInfo laneInfo3 = new LaneInfo(stationId, "lane3", direction, freeway, timestamp,
          lane3Flow, lane3AO, lane3AS, totalFlow);
      c.output(KV.of(stationId, laneInfo3));
      LaneInfo laneInfo4 = new LaneInfo(stationId, "lane4", direction, freeway, timestamp,
          lane4Flow, lane4AO, lane4AS, totalFlow);
      c.output(KV.of(stationId, laneInfo4));
      LaneInfo laneInfo5 = new LaneInfo(stationId, "lane5", direction, freeway, timestamp,
          lane5Flow, lane5AO, lane5AS, totalFlow);
      c.output(KV.of(stationId, laneInfo5));
      LaneInfo laneInfo6 = new LaneInfo(stationId, "lane6", direction, freeway, timestamp,
          lane6Flow, lane6AO, lane6AS, totalFlow);
      c.output(KV.of(stationId, laneInfo6));
      LaneInfo laneInfo7 = new LaneInfo(stationId, "lane7", direction, freeway, timestamp,
          lane7Flow, lane7AO, lane7AS, totalFlow);
      c.output(KV.of(stationId, laneInfo7));
      LaneInfo laneInfo8 = new LaneInfo(stationId, "lane8", direction, freeway, timestamp,
          lane8Flow, lane8AO, lane8AS, totalFlow);
      c.output(KV.of(stationId, laneInfo8));
    }
  }

  /**
   * A custom 'combine function' used with the Combine.perKey transform. Used to find the max lane
   * flow over all the data points in the Window. Extracts the lane flow from the input string and
   * determines whether it's the max seen so far. We're using a custom combiner instead of the Max
   * transform because we want to retain the additional information we've associated with the flow
   * value.
   */
  public static class MaxFlow implements SerializableFunction<Iterable<LaneInfo>, LaneInfo> {
    private static final long serialVersionUID = 0;

    @Override
    public LaneInfo apply(Iterable<LaneInfo> input) {
      Integer max = 0;
      LaneInfo maxInfo = new LaneInfo();
      for (LaneInfo item : input) {
        Integer flow = item.getLaneFlow();
        if (flow != null && (flow >= max)) {
          max = flow;
          maxInfo = item;
        }
      }
      return maxInfo;
    }
  }

  /**
   * Format the results of the Max Lane flow calculation to a TableRow, to save to BigQuery.
   * Add the timestamp from the window context.
   */
  static class FormatMaxesFn extends DoFn<KV<String, LaneInfo>, TableRow> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {

      LaneInfo laneInfo = (LaneInfo) c.element().getValue();
      TableRow row = new TableRow()
          .set("station_id", c.element().getKey())
          .set("direction", laneInfo.getDirection())
          .set("freeway", laneInfo.getFreeway())
          .set("lane_max_flow", laneInfo.getLaneFlow())
          .set("lane", laneInfo.getLane())
          .set("avg_occ", laneInfo.getLaneAO())
          .set("avg_speed", laneInfo.getLaneAS())
          .set("total_flow", laneInfo.getTotalFlow())
          .set("recorded_timestamp", laneInfo.getRecordedTimestamp())
          .set("window_timestamp", c.timestamp().toString());
      c.output(row);
    }

    /** Defines the BigQuery schema used for the output. */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("station_id").setType("STRING"));
      fields.add(new TableFieldSchema().setName("direction").setType("STRING"));
      fields.add(new TableFieldSchema().setName("freeway").setType("STRING"));
      fields.add(new TableFieldSchema().setName("lane_max_flow").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("lane").setType("STRING"));
      fields.add(new TableFieldSchema().setName("avg_occ").setType("FLOAT"));
      fields.add(new TableFieldSchema().setName("avg_speed").setType("FLOAT"));
      fields.add(new TableFieldSchema().setName("total_flow").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("recorded_timestamp").setType("STRING"));
      TableSchema schema = new TableSchema().setFields(fields);
      return schema;
    }
  }

  /**
   * This PTransform extracts lane info, calculates the max lane flow found for a given station (for
   * the current Window) using a custom 'combiner', and formats the results for BigQuery.
   */
  static class MaxLaneFlow
      extends PTransform<PCollection<String>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> apply(PCollection<String> rows) {
      // row... => <stationId, LaneInfo> ...
      PCollection<KV<String, LaneInfo>> flowInfo = rows.apply(
          ParDo.of(new ExtractFlowInfoFn()));

      // stationId, LaneInfo => stationId + max lane flow info
      PCollection<KV<String, LaneInfo>> flowMaxes =
          flowInfo.apply(Combine.<String, LaneInfo>perKey(
              new MaxFlow()));

      // <stationId, max lane flow info>... => row...
      PCollection<TableRow> results = flowMaxes.apply(
          ParDo.of(new FormatMaxesFn()));

      return results;
    }
  }

  /**
    * Options supported by {@link TrafficStreamingMaxLaneFlow}.
    * <p>
    * Inherits standard configuration options.
    */
  private interface TrafficStreamingMaxLaneFlowOptions extends PipelineOptions {
    @Description("Input PubSub topic")
    @Validation.Required
    String getInputTopic();
    void setInputTopic(String value);

    @Description("BigQuery dataset name")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("BigQuery table name")
    @Validation.Required
    String getTable();
    void setTable(String value);

    @Description("Numeric value of sliding window duration, in minutes")
    @Default.Integer(WINDOW_DURATION)
    Integer getWindowDuration();
    void setWindowDuration(Integer value);

    @Description("Numeric value of window 'slide every' setting, in minutes")
    @Default.Integer(WINDOW_SLIDE_EVERY)
    Integer getWindowSlideEvery();
    void setWindowSlideEvery(Integer value);
  }

  /**
   * Sets up and starts streaming pipeline.
   */
  public static void main(String[] args) {
    TrafficStreamingMaxLaneFlowOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(TrafficStreamingMaxLaneFlowOptions.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(dataflowOptions.getProject());
    tableRef.setDatasetId(options.getDataset());
    tableRef.setTableId(options.getTable());
    pipeline
        .apply(PubsubIO.Read.topic(options.getInputTopic()))
        /* map the incoming data stream into sliding windows. The default window duration values
           work well if you're running the accompanying PubSub generator script with the
           --replay flag, which simulates pauses in the sensor data publication. You may want to
           adjust them otherwise. */
        .apply(Window.<String>into(SlidingWindows.of(
            Duration.standardMinutes(options.getWindowDuration())).
            every(Duration.standardMinutes(options.getWindowSlideEvery()))))
        .apply(new MaxLaneFlow())
        .apply(BigQueryIO.Write.to(tableRef)
            .withSchema(FormatMaxesFn.getSchema()));

    /* When you are done running the example, cancel your pipeline so that you do not continue to
       be charged for its instances. You can do this by visiting
       https://console.developers.google.com/project/your-project-name/dataflow/job-id
       in the Developers Console. You should also terminate the generator script so that you do not
       use unnecessary PubSub quota. */
    pipeline.run();
  }

  private static Integer tryIntParse(String number) {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double tryDoubleParse(String number) {
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}

