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

import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Reads real-time NYC taxi ride information from {@code
 * projects/pubsub-public-data/topics/taxirides-realtime} and writes to Iceberg tables using Beam's
 * {@link Managed} IcebergIO sink.
 *
 * <p>This is a streaming pipeline that writes records to Iceberg tables dynamically, depending on
 * each record's passenger count. New tables are created as needed. We set a triggering frequency of
 * 10s; at around this interval, the sink will accumulate records and write them to the appropriate
 * table, creating a new snapshot each time.
 */
public class IcebergTaxiExamples {
  private static final String TAXI_RIDES_TOPIC =
      "projects/pubsub-public-data/topics/taxirides-realtime";
  private static final Schema TAXI_RIDE_INFO_SCHEMA =
      Schema.builder()
          .addStringField("ride_id")
          .addInt32Field("point_idx")
          .addDoubleField("latitude")
          .addDoubleField("longitude")
          .addStringField("timestamp")
          .addDoubleField("meter_reading")
          .addDoubleField("meter_increment")
          .addStringField("ride_status")
          .addInt32Field("passenger_count")
          .build();

  public static void main(String[] args) {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
    options.setProject("apache-beam-testing");

    // each record's 'passenger_count' value will be substituted in to determine
    // its final table destination
    // e.g. an event with 3 passengers will be written to 'iceberg_taxi.3_passengers'
    String tableIdentifierTemplate = "iceberg_taxi.{passenger_count}_passengers";

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("catalog-impl", options.getCatalogImpl())
            .put("warehouse", options.getWarehouse())
            .build();
    Map<String, Object> icebergWriteConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", tableIdentifierTemplate)
            .put("catalog_name", options.getCatalogName())
            .put("catalog_properties", catalogProps)
            .put("triggering_frequency_seconds", 10)
            // perform a final filter to only write these two columns
            .put("keep", Arrays.asList("ride_id", "meter_reading"))
            .build();

    Pipeline p = Pipeline.create(options);
    p
        // Read taxi ride data
        .apply(PubsubIO.readStrings().fromTopic(TAXI_RIDES_TOPIC))
        // Convert JSON strings to Beam Rows
        .apply(JsonToRow.withSchema(TAXI_RIDE_INFO_SCHEMA))
        // Filter to only include drop-offs
        .apply(Filter.<Row>create().whereFieldName("ride_status", "dropoff"::equals))
        // Write to Iceberg tables
        .apply(Managed.write(Managed.ICEBERG).withConfig(icebergWriteConfig));
    p.run();
  }

  public interface IcebergPipelineOptions extends GcpOptions {
    @Description("Warehouse location where the table's data will be written to.")
    @Default.String("gs://apache-beam-samples/iceberg-examples")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("Fully-qualified name of the catalog class to use.")
    @Default.String("org.apache.iceberg.hadoop.HadoopCatalog")
    String getCatalogImpl();

    void setCatalogImpl(String catalogName);

    @Validation.Required
    @Default.String("example-catalog")
    String getCatalogName();

    void setCatalogName(String catalogName);
  }
}
