/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.complete;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A Beam Example that runs in both batch and streaming modes with traffic sensor data. You can
 * configure the running mode by setting {@literal --streaming} to true or false.
 *
 * <p>Concepts: The batch and streaming runners, GroupByKey, sliding windows.
 *
 * <p>This example analyzes traffic sensor data using SlidingWindows. For each window, it calculates
 * the average speed over the window for some small set of predefined 'routes', and looks for
 * 'slowdowns' in those routes. It writes its results to a BigQuery table.
 *
 * <p>The pipeline reads traffic sensor data from {@literal --inputFile}.
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the {@literal
 * --bigQueryDataset}, and {@literal --bigQueryTable} options. If the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class TrafficRoutes {

  // Instantiate some small predefined San Diego routes to analyze
  static Map<String, String> sdStations = buildStationInfo();
  static final int WINDOW_DURATION = 3; // Default sliding window duration in minutes
  static final int WINDOW_SLIDE_EVERY = 1; // Default window 'slide every' setting in minutes

  /** This class holds information about a station reading's average speed. */
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

    @Override
    public boolean equals(Object object) {
      if (object == null) {
        return false;
      }
      if (object.getClass() != getClass()) {
        return false;
      }
      StationSpeed otherStationSpeed = (StationSpeed) object;
      return Objects.equals(this.timestamp, otherStationSpeed.timestamp);
    }

    @Override
    public int hashCode() {
      return this.timestamp.hashCode();
    }
  }

  /** This class holds information about a route's speed/slowdown. */
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

  /** Extract the timestamp field from the input string, and use it as the element timestamp. */
  static class ExtractTimestamps extends DoFn<String, String> {
    private static final DateTimeFormatter dateTimeFormat =
        DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");

    @ProcessElement
    public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
      String[] items = c.element().split(",");
      String timestamp = tryParseTimestamp(items);
      if (timestamp != null) {
        try {
          c.outputWithTimestamp(c.element(), new Instant(dateTimeFormat.parseMillis(timestamp)));
        } catch (IllegalArgumentException e) {
          // Skip the invalid input.
        }
      }
    }
  }

  /**
   * Filter out readings for the stations along predefined 'routes', and output (station, speed
   * info) keyed on route.
   */
  static class ExtractStationSpeedFn extends DoFn<String, KV<String, StationSpeed>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] items = c.element().split(",");
      String stationType = tryParseStationType(items);
      // For this analysis, use only 'main line' station types
      if ("ML".equals(stationType)) {
        Double avgSpeed = tryParseAvgSpeed(items);
        String stationId = tryParseStationId(items);
        // For this simple example, filter out everything but some hardwired routes.
        if (avgSpeed != null && stationId != null && sdStations.containsKey(stationId)) {
          StationSpeed stationSpeed =
              new StationSpeed(stationId, avgSpeed, c.timestamp().getMillis());
          // The tuple key is the 'route' name stored in the 'sdStations' hash.
          KV<String, StationSpeed> outputValue = KV.of(sdStations.get(stationId), stationSpeed);
          c.output(outputValue);
        }
      }
    }
  }

  /**
   * For a given route, track average speed for the window. Calculate whether traffic is currently
   * slowing down, via a predefined threshold. If a supermajority of speeds in this sliding window
   * are less than the previous reading we call this a 'slowdown'. Note: these calculations are for
   * example purposes only, and are unrealistic and oversimplified.
   */
  static class GatherStats extends DoFn<KV<String, Iterable<StationSpeed>>, KV<String, RouteInfo>> {
    @ProcessElement
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

  /** Format the results of the slowdown calculations to a TableRow, to save to BigQuery. */
  static class FormatStatsFn extends DoFn<KV<String, RouteInfo>, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      RouteInfo routeInfo = c.element().getValue();
      TableRow row =
          new TableRow()
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
      return new TableSchema().setFields(fields);
    }
  }

  /**
   * This PTransform extracts speed info from traffic station readings. It groups the readings by
   * 'route' and analyzes traffic slowdown for that route. Lastly, it formats the results for
   * BigQuery.
   */
  static class TrackSpeed
      extends PTransform<PCollection<KV<String, StationSpeed>>, PCollection<TableRow>> {
    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, StationSpeed>> stationSpeed) {
      // Apply a GroupByKey transform to collect a list of all station
      // readings for a given route.
      PCollection<KV<String, Iterable<StationSpeed>>> timeGroup =
          stationSpeed.apply(GroupByKey.create());

      // Analyze 'slowdown' over the route readings.
      PCollection<KV<String, RouteInfo>> stats = timeGroup.apply(ParDo.of(new GatherStats()));

      // Format the results for writing to BigQuery
      PCollection<TableRow> results = stats.apply(ParDo.of(new FormatStatsFn()));

      return results;
    }
  }

  static class ReadFileAndExtractTimestamps extends PTransform<PBegin, PCollection<String>> {
    private final String inputFile;

    public ReadFileAndExtractTimestamps(String inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public PCollection<String> expand(PBegin begin) {
      return begin.apply(TextIO.read().from(inputFile)).apply(ParDo.of(new ExtractTimestamps()));
    }
  }

  /**
   * Options supported by {@link TrafficRoutes}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface TrafficRoutesOptions extends ExampleOptions, ExampleBigQueryTableOptions {
    @Description("Path of the file to read from")
    @Default.String(
        "gs://apache-beam-samples/traffic_sensor/"
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

  public static void runTrafficRoutes(TrafficRoutesOptions options) throws IOException {
    // Using ExampleUtils to set up required resources.
    ExampleUtils exampleUtils = new ExampleUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTable());

    pipeline
        .apply("ReadLines", new ReadFileAndExtractTimestamps(options.getInputFile()))
        // row... => <station route, station speed> ...
        .apply(ParDo.of(new ExtractStationSpeedFn()))
        // map the incoming data stream into sliding windows.
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardMinutes(options.getWindowDuration()))
                    .every(Duration.standardMinutes(options.getWindowSlideEvery()))))
        .apply(new TrackSpeed())
        .apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatStatsFn.getSchema()));

    // Run the pipeline.
    PipelineResult result = pipeline.run();

    // ExampleUtils will try to cancel the pipeline and the injector before the program exists.
    exampleUtils.waitToFinish(result);
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    TrafficRoutesOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TrafficRoutesOptions.class);

    options.setBigQuerySchema(FormatStatsFn.getSchema());

    runTrafficRoutes(options);
  }

  private static Double tryParseAvgSpeed(String[] inputItems) {
    try {
      return Double.parseDouble(tryParseString(inputItems, 9));
    } catch (NumberFormatException | NullPointerException e) {
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
    return inputItems.length > index ? inputItems[index] : null;
  }

  /** Define some small hard-wired San Diego 'routes' to track based on sensor station ID. */
  private static Map<String, String> buildStationInfo() {
    Map<String, String> stations = new LinkedHashMap<>();
    stations.put("1108413", "SDRoute1"); // from freeway 805 S
    stations.put("1108699", "SDRoute2"); // from freeway 78 E
    stations.put("1108702", "SDRoute2");
    return stations;
  }
}
