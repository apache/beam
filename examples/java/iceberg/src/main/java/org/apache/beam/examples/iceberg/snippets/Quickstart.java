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
package org.apache.beam.examples.iceberg.snippets;

import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@SuppressWarnings("all")
public class Quickstart {
  static final String PROJECT_ID = "apache-beam-testing";
  static final String BUCKET_NAME = "my-bucket";

  public static void main(String[] args) {
    // [START hadoop_catalog_props]
    Map<String, String> catalogProps =
        ImmutableMap.of(
            "type", "hadoop",
            "warehouse", "file:///tmp/beam-iceberg-local-quickstart");
    // [END hadoop_catalog_props]
  }

  public static void publicDatasets() {
    // [START biglake_public_catalog_props]
    Map<String, String> catalogProps =
        ImmutableMap.of(
            "type", "rest",
            "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "warehouse", "gs://biglake-public-nyc-taxi-iceberg",
            "header.x-goog-user-project", PROJECT_ID,
            "rest.auth.type", "google",
            "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
            "header.X-Iceberg-Access-Delegation", "vended-credentials");
    // [END biglake_public_catalog_props]

    // [START biglake_public_query]
    Pipeline p = Pipeline.create();

    // Set up query properties:
    Map<String, Object> config =
        ImmutableMap.of(
            "table",
            "public_data.nyc_taxicab",
            "catalog_properties",
            catalogProps,
            "filter",
            "data_file_year = 2021 AND tip_amount > 100",
            "keep",
            Arrays.asList("passenger_count", "total_amount", "trip_distance"));

    // Read Iceberg records
    PCollection<Row> icebergRows =
        p.apply(Managed.read("iceberg").withConfig(config)).getSinglePCollection();

    // Perform further analysis on records
    PCollection<Row> result =
        icebergRows
            .apply(AddFields.<Row>create().field("num_trips", Schema.FieldType.INT32, 1))
            .apply(
                Group.<Row>byFieldNames("passenger_count")
                    .aggregateField("num_trips", Sum.ofIntegers(), "num_trips")
                    .aggregateField("total_amount", Mean.of(), "avg_fare")
                    .aggregateField("trip_distance", Mean.of(), "avg_distance"));

    // Print to console
    result.apply(
        MapElements.into(TypeDescriptors.voids())
            .via(
                row -> {
                  System.out.println(row);
                  return null;
                }));

    // Execute
    p.run().waitUntilFinish();
    // [END biglake_public_query]
  }

  public static void other() {
    // [START biglake_catalog_props]
    Map<String, String> catalogProps =
        ImmutableMap.of(
            "type", "rest",
            "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "warehouse", "gs://" + BUCKET_NAME,
            "header.x-goog-user-project", PROJECT_ID,
            "rest.auth.type", "google",
            "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
            "header.X-Iceberg-Access-Delegation", "vended-credentials");
    // [END biglake_catalog_props]

    // [START managed_iceberg_config]
    Map<String, Object> managedConfig =
        ImmutableMap.of("table", "my_db.my_table", "catalog_properties", catalogProps);

    // Note: The table will get created when inserting data (see below)
    // [END managed_iceberg_config]

    // [START managed_iceberg_insert]
    Schema inputSchema =
        Schema.builder().addInt64Field("id").addStringField("name").addInt32Field("age").build();

    Pipeline p = Pipeline.create();
    p.apply(
            Create.of(
                Row.withSchema(inputSchema).addValues(1, "Mark", 34).build(),
                Row.withSchema(inputSchema).addValues(2, "Omar", 24).build(),
                Row.withSchema(inputSchema).addValues(3, "Rachel", 27).build()))
        .apply(Managed.write("iceberg").withConfig(managedConfig));

    p.run();
    // [END managed_iceberg_insert]

    // [START managed_iceberg_read]
    Pipeline q = Pipeline.create();
    PCollection<Row> rows =
        q.apply(Managed.read("iceberg").withConfig(managedConfig)).getSinglePCollection();

    rows.apply(
        MapElements.into(TypeDescriptors.voids())
            .via(
                row -> {
                  System.out.println(row);
                  return null;
                }));

    q.run();
    // [END managed_iceberg_read]
  }
}
