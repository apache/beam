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
package org.apache.beam.examples.cookbook;

import static org.apache.beam.sdk.managed.Managed.ICEBERG_CDC;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * This pipeline demonstrates how to read a continuous stream of change data capture (CDC) events
 * from an Apache Iceberg table. It processes these events to calculate the hourly total of
 * passengers and writes the aggregated results into a new Iceberg table.
 *
 * <p>This pipeline can be used to process the output of {@link
 * IcebergRestCatalogStreamingWriteExample}.
 *
 * <p>This pipeline also includes a flag {@code --triggerStreamingWrite} which, when enabled, will
 * start the {@link IcebergRestCatalogStreamingWriteExample} in a separate thread to populate the
 * source table. This is useful for execute the end-to-end functionality.
 *
 * <p>This example is a demonstration of the Iceberg REST Catalog. For more information, see the
 * documentation at {@link https://cloud.google.com/bigquery/docs/blms-rest-catalog}.
 *
 * <p>For more information on Apache Beam Iceberg Managed-IO features, see the documentation at
 * {@link https://beam.apache.org/documentation/io/managed-io/}.
 */
public class IcebergRestCatalogCDCExample {

  // Schema for the source table containing minute-level aggregated data
  public static final Schema SOURCE_SCHEMA =
      Schema.builder().addDateTimeField("ride_minute").addInt64Field("passenger_count").build();

  // Schema for the destination table containing hourly aggregated data
  public static final Schema HOURLY_PASSENGER_COUNT_SCHEMA =
      Schema.builder().addDateTimeField("ride_hour").addInt64Field("passenger_count").build();

  public static void main(String[] args) throws IOException {
    IcebergCdcOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergCdcOptions.class);

    if (options.getTriggerStreamingWrite()) {
      new Thread(
              () -> {
                try {
                  IcebergRestCatalogStreamingWriteExample.main(
                      new String[] {
                        "--runner=" + options.getRunner().getSimpleName(),
                        "--project=" + options.getProject(),
                        "--icebergTable=" + options.getSourceTable(),
                        "--catalogUri=" + options.getCatalogUri(),
                        "--warehouse=" + options.getWarehouse(),
                        "--catalogName=" + options.getCatalogName()
                      });
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .start();
    }

    final String sourceTable = options.getSourceTable();
    final String destinationTable = options.getDestinationTable();
    final String catalogUri = options.getCatalogUri();
    final String warehouseLocation = options.getWarehouse();
    final String projectName = options.getProject();
    final String catalogName = options.getCatalogName();
    final int pollIntervalSeconds = 120;
    final int triggeringFrequencySeconds = 30;

    // Note: The token expires in 1 hour. Users may need to re-run the pipeline.
    // Future updates to Iceberg and the BigLake Metastore will support token refreshing.
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", catalogUri)
            .put("warehouse", warehouseLocation)
            .put("header.x-goog-user-project", projectName)
            .put("oauth2-server-uri", "https://oauth2.googleapis.com/token")
            .put(
                "token",
                GoogleCredentials.getApplicationDefault().refreshAccessToken().getTokenValue())
            .put("rest-metrics-reporting-enabled", "false")
            .build();

    Pipeline p = Pipeline.create(options);

    // Configure the Iceberg CDC read
    Map<String, Object> icebergReadConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", sourceTable)
            .put("filter", "\"ride_minute\" IS NOT NULL AND \"passenger_count\" IS NOT NULL")
            .put("keep", ImmutableList.of("ride_minute", "passenger_count"))
            .put("catalog_name", catalogName)
            .put("catalog_properties", catalogProps)
            .put("streaming", true)
            .put("poll_interval_seconds", pollIntervalSeconds)
            .build();

    // Read CDC events from the source Iceberg table
    PCollection<Row> cdcEvents =
        p.apply("ReadFromIceberg", Managed.read(ICEBERG_CDC).withConfig(icebergReadConfig))
            .getSinglePCollection()
            .setRowSchema(SOURCE_SCHEMA);

    // Aggregate passenger counts per hour
    PCollection<Row> aggregatedRows =
        cdcEvents
            .apply(
                "ApplyHourlyWindow",
                Window.<Row>into(FixedWindows.of(Duration.standardMinutes(10))))
            .apply("ExtractHourAndCount", ParDo.of(new ExtractHourAndPassengerCount()))
            .apply("SumPassengerCountPerHour", Sum.longsPerKey())
            .apply("FormatToRow", ParDo.of(new FormatAggregatedRow()))
            .setCoder(RowCoder.of(HOURLY_PASSENGER_COUNT_SCHEMA));

    // Configure the Iceberg write
    Map<String, Object> icebergWriteConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", destinationTable)
            .put("partition_fields", ImmutableList.of("day(ride_hour)"))
            .put("catalog_properties", catalogProps)
            .put("catalog_name", catalogName)
            .put("triggering_frequency_seconds", triggeringFrequencySeconds)
            .build();

    // Write the aggregated results to the destination Iceberg table
    aggregatedRows.apply(
        "WriteToIceberg", Managed.write(Managed.ICEBERG).withConfig(icebergWriteConfig));

    p.run().waitUntilFinish();
  }

  private static class FormatAggregatedRow extends DoFn<KV<String, Long>, Row> {
    @ProcessElement
    public void processElement(@Element KV<String, Long> kv, OutputReceiver<Row> out) {
      Row row =
          Row.withSchema(HOURLY_PASSENGER_COUNT_SCHEMA)
              .withFieldValue("ride_hour", DateTime.parse(kv.getKey()))
              .withFieldValue("passenger_count", kv.getValue())
              .build();
      out.output(row);
    }
  }

  private static class ExtractHourAndPassengerCount extends DoFn<Row, KV<String, Long>> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<KV<String, Long>> out) {
      DateTime rideHour =
          ((DateTime) Preconditions.checkStateNotNull(row.getDateTime("ride_minute")))
              .withSecondOfMinute(0)
              .withMillisOfSecond(0);
      out.output(
          KV.of(
              rideHour.toString(),
              Preconditions.checkStateNotNull(row.getInt64("passenger_count"))));
    }
  }

  /** Pipeline options for this example. */
  public interface IcebergCdcOptions extends GcpOptions {
    @Description("The source Iceberg table to read CDC events from")
    @Default.String("taxi_dataset.ride_metrics_by_minute")
    String getSourceTable();

    void setSourceTable(String sourceTable);

    @Description("The destination Iceberg table to write aggregated results to")
    @Default.String("taxi_dataset.passenger_count_by_hour")
    String getDestinationTable();

    void setDestinationTable(String destinationTable);

    @Description("Warehouse location for the Iceberg catalog")
    @Default.String("gs://biglake_taxi_ride_metrics")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The URI for the REST catalog")
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The name of the Iceberg catalog")
    @Default.String("taxi_rides")
    String getCatalogName();

    void setCatalogName(String catalogName);

    @Description("Trigger the streaming write example")
    @Default.Boolean(false)
    boolean getTriggerStreamingWrite();

    void setTriggerStreamingWrite(boolean triggerStreamingWrite);
  }
}
