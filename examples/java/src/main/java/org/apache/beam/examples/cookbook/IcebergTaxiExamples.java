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
import org.apache.beam.sdk.io.iceberg.IcebergIO;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads real-time NYC taxi ride information from {@code
 * projects/pubsub-public-data/topics/taxirides-realtime} and writes to Iceberg tables using Beam's
 * {@link Managed} {@link IcebergIO} sink.
 *
 * <p>We'd like to have a distribution of taxi ride cost for each case of number of passengers. A
 * car can have up to six passengers, so first we create six Iceberg tables to represent each case.
 * Then we execute the pipeline, which writes records to Iceberg tables dynamically depending on
 * each record's passenger count.
 *
 * <p>In this streaming pipeline, we set a triggering frequency of 10s. Around this interval, the
 * Iceberg sink will write accumulated records and create a new snapshot for each table.
 */
public class IcebergTaxiExamples {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTaxiExamples.class);
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
    String tableIdentifierTemplate = "iceberg_taxi.{passenger_count}_passengers";

    createTables(options, tableIdentifierTemplate);

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
    p.run().waitUntilFinish();
  }

  private static void createTables(IcebergPipelineOptions options, String tableIdentifierTemplate) {
    Catalog catalog =
        CatalogUtil.loadCatalog(
            options.getCatalogImpl(),
            options.getCatalogName(),
            ImmutableMap.of("warehouse", options.getWarehouse()),
            new Configuration());

    org.apache.iceberg.Schema tableSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ride_id", Types.StringType.get()),
            Types.NestedField.required(2, "meter_reading", Types.DoubleType.get()));
    for (int i = 1; i <= 6; i++) {
      String table = tableIdentifierTemplate.replace("{passenger_count}", String.valueOf(i));
      TableIdentifier identifier = TableIdentifier.parse(table);
      try {
        catalog.createTable(identifier, tableSchema);
        LOG.info("Successfully created table '{}'", identifier);
      } catch (AlreadyExistsException e) {
        if (catalog.loadTable(identifier).schema().sameSchema(tableSchema)) {
          LOG.info("Table already exists: '{}'", identifier);
          continue;
        }
        throw new RuntimeException("Table exists with a different schema: " + identifier, e);
      }
    }
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
