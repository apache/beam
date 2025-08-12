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

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * Reads real-time NYC taxi ride information from {@code
 * projects/pubsub-public-data/topics/taxirides-realtime} and writes aggregated metrics data to an
 * Iceberg table using Beam's {@link Managed} IcebergIO sink.
 *
 * <p>This is a streaming pipeline that processes taxi ride events, filters for 'dropoff' status,
 * aggregates metrics within fixed 10-second windows by minute of the ride, and writes the results
 * to a single Iceberg table. The Iceberg sink triggers writes every 30 seconds, creating new
 * snapshots.
 *
 * <p>This example is a demonstration of the Iceberg REST Catalog. For more information, see the
 * documentation at {@link https://cloud.google.com/bigquery/docs/blms-rest-catalog}.
 */
public class IcebergRestCatalogStreamingWriteExample {

  // Schema for the incoming taxi ride data from Pub/Sub
  public static final Schema TAXIRIDES_SCHEMA =
      Schema.builder()
          .addStringField("ride_id")
          .addStringField("ride_status")
          .addDateTimeField("timestamp")
          .addInt64Field("passenger_count")
          .addDoubleField("meter_reading")
          .build();

  // Schema for the aggregated data to be written to Iceberg
  public static final Schema AGGREGATED_SCHEMA =
      Schema.builder()
          .addDateTimeField("ride_minute")
          .addInt64Field("total_rides")
          .addDoubleField("revenue")
          .addInt64Field("passenger_count")
          .build();

  public static final String TAXI_RIDES_TOPIC =
      "projects/pubsub-public-data/topics/taxirides-realtime";

  /**
   * Checks if a {@link Row} contains all required fields.
   *
   * @param row The input Row
   * @return {@code true} if all fields are present, {@code false} otherwise
   */
  private static boolean areAllFieldsPresent(Row row) {
    return row.getString("ride_id") != null
        && row.getString("ride_status") != null
        && row.getDateTime("timestamp") != null
        && row.getInt64("passenger_count") != null
        && row.getDouble("meter_reading") != null;
  }

  /**
   * The main entry point for the pipeline.
   *
   * @param args Command-line arguments
   * @throws IOException If there is an issue with Google Credentials
   */
  public static void main(String[] args) throws IOException {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergPipelineOptions.class);

    final String tableIdentifier = options.getIcebergTable();
    final String catalogUri = options.getCatalogUri();
    final String warehouseLocation = options.getWarehouse();
    final String projectName = options.getProject();
    final String catalogName = options.getCatalogName();
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

    Map<String, Object> icebergWriteConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", tableIdentifier)
            .put("catalog_properties", catalogProps)
            .put("catalog_name", catalogName)
            .put("triggering_frequency_seconds", triggeringFrequencySeconds)
            .build();

    Pipeline p = Pipeline.create(options);

    p.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(TAXI_RIDES_TOPIC))
        .apply("ConvertJsonToRow", JsonToRow.withSchema(TAXIRIDES_SCHEMA))
        .apply("FilterNullFields", Filter.by(r -> areAllFieldsPresent(r)))
        .apply("FilterDropoffRides", Filter.by(r -> "dropoff".equals(r.getString("ride_status"))))
        .apply("ApplyFixedWindow", Window.<Row>into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(
            "ExtractMinuteAsKey",
            WithKeys.<String, Row>of(
                (Row row) ->
                    ((DateTime) Preconditions.checkStateNotNull(row.getDateTime("timestamp")))
                        .withSecondOfMinute(0)
                        .withMillisOfSecond(0)
                        .toString()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(TAXIRIDES_SCHEMA)))
        .apply("AggregateMetrics", Combine.<String, Row, Accum>perKey(new AggregateMetricsFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Accum.class)))
        .apply("FormatForIceberg", ParDo.of(new FormatRowFn()))
        .setCoder(RowCoder.of(AGGREGATED_SCHEMA))
        .apply("WriteToIceberg", Managed.write(Managed.ICEBERG).withConfig(icebergWriteConfig));

    p.run().waitUntilFinish();
  }

  private static class FormatRowFn extends DoFn<KV<String, Accum>, Row> {
    @ProcessElement
    public void processElement(@Element KV<String, Accum> element, OutputReceiver<Row> out) {
      Accum metrics = Preconditions.checkStateNotNull(element.getValue());
      DateTime rideMinute = DateTime.parse(element.getKey());
      Row row =
          Row.withSchema(AGGREGATED_SCHEMA)
              .withFieldValue("ride_minute", rideMinute)
              .withFieldValue("total_rides", metrics.getTotalRides())
              .withFieldValue("revenue", metrics.getRevenue())
              .withFieldValue("passenger_count", metrics.getPassengerCount())
              .build();
      out.output(row);
    }
  }

  public static class Accum implements Serializable {
    private final long totalRides;
    private final double revenue;
    private final long passengerCount;

    public Accum(long totalRides, double revenue, long passengerCount) {
      this.totalRides = totalRides;
      this.revenue = revenue;
      this.passengerCount = passengerCount;
    }

    public long getTotalRides() {
      return totalRides;
    }

    public double getRevenue() {
      return revenue;
    }

    public long getPassengerCount() {
      return passengerCount;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Accum accum = (Accum) o;
      return totalRides == accum.totalRides
          && Double.compare(accum.revenue, revenue) == 0
          && passengerCount == accum.passengerCount;
    }

    @Override
    public int hashCode() {
      return Objects.hash(totalRides, revenue, passengerCount);
    }
  }

  public static class AggregateMetricsFn extends Combine.CombineFn<Row, Accum, Accum> {
    @Override
    public Accum createAccumulator() {
      return new Accum(0, 0, 0);
    }

    @Override
    public Accum addInput(Accum mutableAccumulator, Row input) {
      return new Accum(
          mutableAccumulator.getTotalRides() + 1,
          mutableAccumulator.getRevenue()
              + Preconditions.checkStateNotNull(input.getDouble("meter_reading")),
          mutableAccumulator.getPassengerCount()
              + Preconditions.checkStateNotNull(input.getInt64("passenger_count")));
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {
      long totalRides = 0;
      double revenue = 0;
      long passengerCount = 0;
      for (Accum accum : accumulators) {
        totalRides += accum.getTotalRides();
        revenue += accum.getRevenue();
        passengerCount += accum.getPassengerCount();
      }
      return new Accum(totalRides, revenue, passengerCount);
    }

    @Override
    public Accum extractOutput(Accum accumulator) {
      return accumulator;
    }
  }

  /** Pipeline options for the IcebergRestCatalogStreamingWriteExample. */
  public interface IcebergPipelineOptions extends GcpOptions {
    @Description(
        "Warehouse location where the table's data will be written to. "
            + "As of 07/14/25 BigLake only supports Single Region buckets")
    @Validation.Required
    @Default.String("gs://biglake_taxi_ride_metrics")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The URI for the REST catalog.")
    @Validation.Required
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The iceberg table to write to.")
    @Validation.Required
    @Default.String("taxi_dataset.ride_metrics_by_minute")
    String getIcebergTable();

    void setIcebergTable(String value);

    @Validation.Required
    @Default.String("taxi_rides")
    String getCatalogName();

    void setCatalogName(String catalogName);
  }
}
