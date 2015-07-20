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

package com.google.cloud.dataflow.examples.complete;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.examples.common.DataflowExampleOptions;
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils;
import com.google.cloud.dataflow.examples.common.ExampleBigQueryTableOptions;
import com.google.cloud.dataflow.examples.common.ExamplePubsubTopicOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * A Dataflow Example that runs in both batch and streaming modes with traffic sensor data.
 * You can configure the running mode by setting {@literal --streaming} to true or false.
 *
 * <p>Concepts: The batch and streaming runners, GroupByKey, sliding windows, and
 * Google Cloud Pub/Sub topic injection.
 *
 * <p> This example analyzes traffic sensor data using SlidingWindows. For each window,
 * it calculates the average speed over the window for some small set of predefined 'routes',
 * and looks for 'slowdowns' in those routes. It writes its results to a BigQuery table.
 *
 * <p> In batch mode, the pipeline reads traffic sensor data from {@literal --inputFile}.
 *
 * <p> In streaming mode, the pipeline reads the data from a Pub/Sub topic.
 * By default, the example will run a separate pipeline to inject the data from the default
 * {@literal --inputFile} to the Pub/Sub {@literal --pubsubTopic}. It will make it available for
 * the streaming pipeline to process. You may override the default {@literal --inputFile} with the
 * file of your choosing. You may also set {@literal --inputFile} to an empty string, which will
 * disable the automatic Pub/Sub injection, and allow you to use separate tool to control the input
 * to this example. An example code, which publishes traffic sensor data to a Pub/Sub topic,
 * is provided in
 * <a href="https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/tree/master/gce-cmdline-publisher"></a>.
 *
 * <p> The example is configured to use the default Pub/Sub topic and the default BigQuery table
 * from the example common package (there are no defaults for a general Dataflow pipeline).
 * You can override them by using the {@literal --pubsubTopic}, {@literal --bigQueryDataset}, and
 * {@literal --bigQueryTable} options. If the Pub/Sub topic or the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p> The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */

public class TrafficRoutes {

  // Instantiate some small predefined San Diego routes to analyze
  static Map<String, String> sdStations = buildStationInfo();
  static final int WINDOW_DURATION = 3;  // Default sliding window duration in minutes
  static final int WINDOW_SLIDE_EVERY = 1;  // Default window 'slide every' setting in minutes

  /**
   * This class holds information about a station reading's average speed.
   */
  @DefaultCoder(AvroCoder.class)
  static class StationSpeed implements Comparable<StationSpeed> {
    @Nullable String stationId;
    @Nullable Double avgSpeed;
    @Nullable Long timestamp;

    public StationSpeed() {}

    public StationSpeed(String stationId, Double avgSpeed, Long timestamp) {
      this.stationId = stationId;
      this.avgSpeed = avgSpeed;
      this.timestamp = timestamp;
    }

    public String getStationId() {
      return this.stationId;
    }
    public Double getAvgSpeed() {
      return this.avgSpeed;
    }

    @Override
    public int compareTo(StationSpeed other) {
      return Long.compare(this.timestamp, other.timestamp);
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
    private static final DateTimeFormatter dateTimeFormat =
        DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");

    private final boolean outputTimestamp;

    public ExtractStationSpeedFn(boolean outputTimestamp) {
      this.outputTimestamp = outputTimestamp;
    }


    @Override
    public void processElement(ProcessContext c) {
      String[] items = c.element().split(",");
      String stationType = tryParseStationType(items);
      // For this analysis, use only 'main line' station types
      if (stationType != null && stationType.equals("ML")) {
        Double avgSpeed = tryParseAvgSpeed(items);
        String stationId = tryParseStationId(items);
        // For this simple example, filter out everything but some hardwired routes.
        if (avgSpeed != null && stationId != null && sdStations.containsKey(stationId)) {
          Instant timestamp;
          if (outputTimestamp) {
            timestamp = new Instant(dateTimeFormat.parseMillis(tryParseTimestamp(items)));
          } else {
            timestamp = c.timestamp();
          }
          StationSpeed stationSpeed = new StationSpeed(stationId, avgSpeed, timestamp.getMillis());
          // The tuple key is the 'route' name stored in the 'sdStations' hash.
          KV<String, StationSpeed> outputValue = KV.of(sdStations.get(stationId), stationSpeed);
          if (outputTimestamp) {
            c.outputWithTimestamp(outputValue, timestamp);
          } else {
            c.output(outputValue);
          }
        }
      }
    }
  }

  /**
   * For a given route, track average speed for the window. Calculate whether
   * traffic is currently slowing down, via a predefined threshold. If a supermajority of
   * speeds in this sliding window are less than the previous reading we call this a 'slowdown'.
   * Note: these calculations are for example purposes only, and are unrealistic and oversimplified.
   */
  static class GatherStats
      extends DoFn<KV<String, Iterable<StationSpeed>>, KV<String, RouteInfo>> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) throws IOException {
      String route = c.element().getKey();
      double speedSum = 0.0;
      int speedCount = 0;
      int speedups = 0;
      int slowdowns = 0;
      List<StationSpeed> infoList = Lists.newArrayList(c.element().getValue());
      // StationSpeeds sort by embedded timestamp.
      Collections.sort(infoList);
      Map<String, Double> prevSpeeds = new HashMap<>();
      // For all stations in the route, sum (non-null) speeds. Keep a count of the non-null speeds.
      for (StationSpeed item : infoList) {
        Double speed = item.getAvgSpeed();
        if (speed != null) {
          speedSum += speed;
          speedCount++;
          Double lastSpeed = prevSpeeds.get(item.getStationId());
          if (lastSpeed != null) {
            if (lastSpeed < speed) {
              speedups += 1;
            } else {
              slowdowns += 1;
            }
          }
          prevSpeeds.put(item.getStationId(), speed);
        }
      }
      if (speedCount == 0) {
        // No average to compute.
        return;
      }
      double speedAvg = speedSum / speedCount;
      boolean slowdownEvent = slowdowns >= 2 * speedups;
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

    /**
     * Defines the BigQuery schema used for the output.
     */
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
   * It groups the readings by 'route' and analyzes traffic slowdown for that route.
   * Lastly, it formats the results for BigQuery.
   */
  static class TrackSpeed extends
      PTransform<PCollection<KV<String, StationSpeed>>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> apply(PCollection<KV<String, StationSpeed>> stationSpeed) {
      // Apply a GroupByKey transform to collect a list of all station
      // readings for a given route.
      PCollection<KV<String, Iterable<StationSpeed>>> timeGroup = stationSpeed.apply(
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
  * Options supported by {@link TrafficRoutes}.
  *
  * <p> Inherits standard configuration options.
  */
  private interface TrafficRoutesOptions
      extends DataflowExampleOptions, ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {
    @Description("Input file to inject to Pub/Sub topic")
    @Default.String("gs://dataflow-samples/traffic_sensor/"
        + "Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv")
    String getInputFile();
    void setInputFile(String value);

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
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    TrafficRoutesOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(TrafficRoutesOptions.class);

    if (options.isStreaming()) {
      // In order to cancel the pipelines automatically,
      // {@literal DataflowPipelineRunner} is forced to be used.
      options.setRunner(DataflowPipelineRunner.class);
    }
    options.setBigQuerySchema(FormatStatsFn.getSchema());
    // Using DataflowExampleUtils to set up required resources.
    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);
    dataflowUtils.setup();

    Pipeline pipeline = Pipeline.create(options);
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTable());

    PCollection<KV<String, StationSpeed>> input;
    if (options.isStreaming()) {
      input = pipeline
          .apply(PubsubIO.Read.topic(options.getPubsubTopic()))
          // row... => <station route, station speed> ...
          .apply(ParDo.of(new ExtractStationSpeedFn(false /* outputTimestamp */)));
    } else {
      input = pipeline
          .apply(TextIO.Read.from(options.getInputFile()))
          .apply(ParDo.of(new ExtractStationSpeedFn(true /* outputTimestamp */)));
    }

    // map the incoming data stream into sliding windows.
    // The default window duration values work well if you're running the accompanying Pub/Sub
    // generator script without the --replay flag, so that there are no simulated pauses in
    // the sensor data publication. You may want to adjust the values otherwise.
    input.apply(Window.<KV<String, StationSpeed>>into(SlidingWindows.of(
            Duration.standardMinutes(options.getWindowDuration())).
            every(Duration.standardMinutes(options.getWindowSlideEvery()))))
        .apply(new TrackSpeed())
        .apply(BigQueryIO.Write.to(tableRef)
            .withSchema(FormatStatsFn.getSchema()));

    PipelineResult result = pipeline.run();
    if (options.isStreaming() && !options.getInputFile().isEmpty()) {
      // Inject the data into the Pub/Sub topic with a Dataflow batch pipeline.
      dataflowUtils.runInjectorPipeline(options.getInputFile(), options.getPubsubTopic());
    }

    // dataflowUtils will try to cancel the pipeline and the injector before the program exists.
    dataflowUtils.waitToFinish(result);
  }

  private static Double tryParseAvgSpeed(String[] inputItems) {
    try {
      return Double.parseDouble(tryParseString(inputItems, 9));
    } catch (NumberFormatException e) {
      return null;
    } catch (NullPointerException e) {
      return null;
    }
  }

  private static String tryParseStationType(String[] inputItems) {
    return tryParseString(inputItems, 4);
  }

  private static String tryParseStationId(String[] inputItems) {
    return tryParseString(inputItems, 1);
  }

  private static String tryParseTimestamp(String[] inputItems) {
    return tryParseString(inputItems, 0);
  }

  private static String tryParseString(String[] inputItems, int index) {
    return inputItems.length >= index ? inputItems[index] : null;
  }

  /**
   * Define some small hard-wired San Diego 'routes' to track based on sensor station ID.
   */
  private static Map<String, String> buildStationInfo() {
    Map<String, String> stations = new Hashtable<String, String>();
      stations.put("1108413", "SDRoute1"); // from freeway 805 S
      stations.put("1108699", "SDRoute2"); // from freeway 78 E
      stations.put("1108702", "SDRoute2");
    return stations;
  }
}
