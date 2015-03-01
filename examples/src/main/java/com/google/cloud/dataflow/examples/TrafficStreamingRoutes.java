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
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.MoreObjects;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;


/**
 * A streaming Dataflow Example using BigQuery output, in the 'traffic sensor' domain.
 *
 * <p>Concepts: The streaming runner, GroupByKey, keyed state, sliding windows, and
 * PubSub topic ingestion.
 *
 * <p> This pipeline takes as input traffic sensor data from a PubSub topic, and analyzes it using
 * SlidingWindows. For each window, it calculates the average speed over the window for some small
 * set of predefined 'routes', and looks for 'slowdowns' in those routes. It uses keyed state to
 * track slowdown information across successive sliding windows. It writes its results to a
 * BigQuery table.
 *
 * <p> This pipeline expects input from
 * <a href="https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/tree/master/gce-cmdline-publisher">
 * this script</a>,
 * which publishes traffic sensor data to a PubSub topic. After you've started this pipeline, start
 * up the input generation script as per its instructions. The default SlidingWindow parameters
 * assume that you're running this script without the {@literal --replay} flag, so that there are
 * no simulated pauses in the sensor data publication.
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
public class TrafficStreamingRoutes {
  // Instantiate some small predefined San Diego routes to analyze
  static Map<String, String> sdStations = buildStationInfo();
  static final int WINDOW_DURATION = 3;  // Default sliding window duration in minutes
  static final int WINDOW_SLIDE_EVERY = 1;  // Default window 'slide every' setting in minutes

  /**
   * This class holds information about a station reading's average speed.
   */
  @DefaultCoder(AvroCoder.class)
  static class StationSpeed {
    @Nullable String stationId;
    @Nullable Double avgSpeed;

    public StationSpeed() {}

    public StationSpeed(String stationId, Double avgSpeed) {
      this.stationId = stationId;
      this.avgSpeed = avgSpeed;
    }

    public String getStationId() {
      return this.stationId;
    }
    public Double getAvgSpeed() {
      return this.avgSpeed;
    }
  }

  /**
   * This class holds information about a route's speed/slowdown.
   */
  @DefaultCoder(AvroCoder.class)
  static class RouteInfo {
    @Nullable String route;
    @Nullable Double avgSpeed;
    @Nullable Boolean slowdownEvent;


    public RouteInfo() {}

    public RouteInfo(String route, Double avgSpeed, Boolean slowdownEvent) {
      this.route = route;
      this.avgSpeed = avgSpeed;
      this.slowdownEvent = slowdownEvent;
    }

    public String getRoute() {
      return this.route;
    }
    public Double getAvgSpeed() {
      return this.avgSpeed;
    }
    public Boolean getSlowdownEvent() {
      return this.slowdownEvent;
    }
  }

  /**
   * Filter out readings for the stations along predefined 'routes', and output
   * (station, speed info) keyed on route.
   */
  static class ExtractStationSpeedFn extends DoFn<String, KV<String, StationSpeed>> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      String[] items = c.element().split(",");
      String stationId = items[1];
      String stationType = items[4];
      Double avgSpeed = tryDoubleParse(items[9]);
      // For this analysis, use only 'main line' station types
      if (stationType.equals("ML")) {
        // For this simple example, filter out everything but some hardwired routes.
        if (sdStations.containsKey(stationId)) {
          StationSpeed stationSpeed = new StationSpeed(stationId, avgSpeed);
          // The tuple key is the 'route' name stored in the 'sdStations' hash.
          c.output(KV.of(sdStations.get(stationId), stationSpeed));
        }
      }
    }
  }

  /*
   * For a given route, track average speed for the window. Calculate whether traffic is currently
   * slowing down, via a predefined threshold. Use keyed state to keep a count of the speed drops,
   * with at least 3 in a row constituting a 'slowdown'.
   * Note: these calculations are for example purposes only, and are unrealistic and oversimplified.
   */
  static class GatherStats extends DoFn<KV<String, Iterable<StationSpeed>>, KV<String, RouteInfo>>
    implements DoFn.RequiresKeyedState {
    private static final long serialVersionUID = 0;

    static final int SLOWDOWN_THRESH = 67;
    static final int SLOWDOWN_COUNT_CAP = 3;

    @Override
    public void processElement(ProcessContext c) throws IOException {
      String route = c.element().getKey();
      CodedTupleTag<Integer> tag = CodedTupleTag.of(route, BigEndianIntegerCoder.of());
      // For the given key (a route), get the keyed state.
      Integer slowdownCount = MoreObjects.firstNonNull(c.keyedState().lookup(tag), 0);
      Double speedSum = 0.0;
      Integer scount = 0;
      Iterable<StationSpeed> infoList = c.element().getValue();
      // For all stations in the route, sum (non-null) speeds. Keep a count of the non-null speeds.
      for (StationSpeed item : infoList) {
        Double speed = item.getAvgSpeed();
        if (speed != null) {
          speedSum += speed;
          scount++;
        }
      }
      // calculate average speed.
      if (scount == 0) {
        return;
      }
      Double speedAvg = speedSum / scount;
      Boolean slowdownEvent = false;
      if (speedAvg != null) {
        // see if the speed falls below defined threshold. If it does, increment the count of
        // slow readings, as retrieved from the keyed state, up to the defined cap.
        if (speedAvg < SLOWDOWN_THRESH) {
          if (slowdownCount < SLOWDOWN_COUNT_CAP) {
            slowdownCount++;
          }
        } else if (slowdownCount > 0) {
          // if speed is not below threshold, then decrement the count of slow readings.
          slowdownCount--;
        }
        // if our count of slowdowns has reached its cap, we consider this a 'slowdown event'
        if (slowdownCount >= SLOWDOWN_COUNT_CAP) {
          slowdownEvent = true;
        }
      }
      // store the new slowdownCount in the keyed state for the route key.
      c.keyedState().store(tag, slowdownCount);
      RouteInfo routeInfo = new RouteInfo(route, speedAvg, slowdownEvent);
      c.output(KV.of(route, routeInfo));
    }
  }

  /**
   * Format the results of the slowdown calculations to a TableRow, to save to BigQuery.
   */
  static class FormatStatsFn extends DoFn<KV<String, RouteInfo>, TableRow> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      RouteInfo routeInfo = c.element().getValue();
      TableRow row = new TableRow()
          .set("avg_speed", routeInfo.getAvgSpeed())
          .set("slowdown_event", routeInfo.getSlowdownEvent())
          .set("route", c.element().getKey())
          .set("window_timestamp", c.timestamp().toString());
      c.output(row);
    }

    /** Defines the BigQuery schema used for the output. */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("route").setType("STRING"));
      fields.add(new TableFieldSchema().setName("avg_speed").setType("FLOAT"));
      fields.add(new TableFieldSchema().setName("slowdown_event").setType("BOOLEAN"));
      fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
      TableSchema schema = new TableSchema().setFields(fields);
      return schema;
    }
  }

  /**
   * This PTransform extracts speed info from traffic station readings.
   * It groups the readings by 'route' and analyzes traffic slowdown for that route, using keyed
   * state to retain previous slowdown information. Then, it formats the results for BigQuery.
   */
  static class TrackSpeed extends PTransform<PCollection<String>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> apply(PCollection<String> rows) {
      // row... => <station route, station speed> ...
      PCollection<KV<String, StationSpeed>> flowInfo = rows.apply(
          ParDo.of(new ExtractStationSpeedFn()));

      // Apply a GroupByKey transform to collect a list of all station
      // readings for a given route.
      PCollection<KV<String, Iterable<StationSpeed>>> timeGroup = flowInfo.apply(
        GroupByKey.<String, StationSpeed>create());

      // Analyze 'slowdown' over the route readings.
      PCollection<KV<String, RouteInfo>> stats = timeGroup.apply(ParDo.of(new GatherStats()));

      // Format the results for writing to BigQuery
      PCollection<TableRow> results = stats.apply(
          ParDo.of(new FormatStatsFn()));

      return results;
    }
  }


  /**
  * Options supported by {@link TrafficStreamingRoutes}.
  * <p>
  * Inherits standard configuration options.
  */
  private interface TrafficStreamingRoutesOptions extends PipelineOptions {
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
    TrafficStreamingRoutesOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(TrafficStreamingRoutesOptions.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(dataflowOptions.getProject());
    tableRef.setDatasetId(options.getDataset());
    tableRef.setTableId(options.getTable());
    pipeline
        .apply(PubsubIO.Read.topic(options.getInputTopic()))
        /* map the incoming data stream into sliding windows.
           The default window duration values work well if you're running the accompanying PubSub
           generator script without the --replay flag, so that there are no simulated pauses in
           the sensor data publication. You may want to adjust the values otherwise. */
        .apply(Window.<String>into(SlidingWindows.of(
            Duration.standardMinutes(options.getWindowDuration())).
            every(Duration.standardMinutes(options.getWindowSlideEvery()))))
        .apply(new TrackSpeed())
        .apply(BigQueryIO.Write.to(tableRef)
            .withSchema(FormatStatsFn.getSchema()));

    /* When you are done running the example, cancel your pipeline so that you do not continue to
       be charged for its instances. You can do this by visiting
       https://console.developers.google.com/project/your-project-name/dataflow/job-id
       in the Developers Console. You should also terminate the generator script so that you do not
       use unnecessary PubSub quota. */
    pipeline.run();
  }

  private static Double tryDoubleParse(String number) {
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Define some small hard-wired San Diego 'routes' to track based on sensor station ID. */
  private static Map<String, String> buildStationInfo() {
    Map<String, String> stations = new Hashtable<String, String>();
      stations.put("1108413", "SDRoute1"); // from freeway 805 S
      stations.put("1108699", "SDRoute2"); // from freeway 78 E
      stations.put("1108702", "SDRoute2");
    return stations;
  }

}

