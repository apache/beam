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
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * Reads real-time NYC taxi ride information from {@code
 * projects/pubsub-public-data/topics/taxirides-realtime} and writes aggregated passenger count data
 * to an Iceberg table using Beam's {@link Managed} IcebergIO sink.
 *
 * <p>This is a streaming pipeline that processes taxi ride events, filters for 'dropoff' status,
 * aggregates passenger counts within fixed 10-second windows by minute of the ride, and writes the
 * results to a single Iceberg table. The Iceberg sink triggers writes every 30 seconds, creating
 * new snapshots.
 *
 * <p>This example is a demonstration of the Iceberg REST Catalog. For more information, see the
 * documentation at {@link https://cloud.google.com/bigquery/docs/blms-rest-catalog}.
 */
public class IcebergRestCatalogStreamingWriteExample {

  public static final Schema TAXIRIDES_SCHEMA =
      Schema.builder()
          .addInt64Field("passenger_count")
          .addStringField("ride_status")
          .addDateTimeField("timestamp")
          .build();

  public static final Schema AGGREGATED_SCHEMA =
      Schema.builder().addStringField("ride_minute").addInt64Field("passenger_count").build();

  /**
   * Main entry point for the pipeline.
   *
   * @param args Command line arguments
   * @throws IOException if there's an issue with GoogleCredentials
   */
  public static void main(String[] args) throws IOException {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergPipelineOptions.class);
    options.setProject("apache-beam-testing");

    final String tableIdentifier = "taxi_dataset.passenger_count_by_minute";
    final String pubsubTopic = options.getTopic();
    final String catalogUri = options.getCatalogUri();
    final String warehouseLocation = options.getWarehouse();
    final String projectName = options.getProject();
    final String catalogName = options.getCatalogName();
    final int triggeringFrequencySeconds = 30;

    // Note: The token expires in 1 hour, and users may need to re-run the pipeline.
    // Upcoming changes in Iceberg and the BigLake Metastore with the Iceberg REST Catalog
    // will support token refreshing and credential vending.
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

    PCollection<Row> aggregatedRows =
        p.apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(pubsubTopic))
            .apply("ConvertJsonToRow", JsonToRow.withSchema(TAXIRIDES_SCHEMA))
            .apply(
                "FilterNullFields",
                Filter.by(
                    (Row row) ->
                        row.getInt64("passenger_count") != null
                            && row.getDateTime("timestamp") != null))
            .apply(
                "FilterDropoffRides",
                Filter.by((Row row) -> "dropoff".equals(row.getString("ride_status"))))
            .apply(
                "ApplyFixedWindow", Window.<Row>into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(
                "ExtractMinuteAndPassengerCount",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                    .via(
                        (Row row) -> {
                          Long passengerCount = row.getInt64("passenger_count");
                          DateTime timestamp =
                              (DateTime)
                                  Preconditions.checkStateNotNull(row.getDateTime("timestamp"));
                          String minute = timestamp.toString("yyyy-MM-dd HH:mm");
                          return KV.of(minute, passengerCount);
                        }))
            .apply("SumPassengerCountPerMinute", Sum.longsPerKey())
            .apply(
                "FormatAggregatedRowForIceberg",
                ParDo.of(
                    new DoFn<KV<String, Long>, Row>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, Long> kv, OutputReceiver<Row> out) {
                        Row row =
                            Row.withSchema(AGGREGATED_SCHEMA)
                                .withFieldValue("ride_minute", kv.getKey())
                                .withFieldValue("passenger_count", kv.getValue())
                                .build();
                        out.output(row);
                      }
                    }))
            .setCoder(RowCoder.of(AGGREGATED_SCHEMA));

    aggregatedRows.apply(
        "WriteToIceberg", Managed.write(Managed.ICEBERG).withConfig(icebergWriteConfig));

    p.run().waitUntilFinish();
  }

  /** Pipeline options for the IcebergRestCatalogStreamingWriteExample. */
  public interface IcebergPipelineOptions extends GcpOptions {
    @Description(
        "Warehouse location where the table's data will be written to. "
            + "As of 07/14/25 BigLake only supports Single Region buckets")
    @Validation.Required
    @Default.String("gs://biglake_taxi_rides")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The URI for the REST catalog.")
    @Validation.Required
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The Pub/Sub topic to read from.")
    @Validation.Required
    @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
    String getTopic();

    void setTopic(String value);

    @Validation.Required
    @Default.String("taxi_rides")
    String getCatalogName();

    void setCatalogName(String catalogName);
  }
}
