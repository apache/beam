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
package org.apache.beam.sdk.io.iceberg;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Reads a managed Apache Iceberg table in BigQuery through {@code Managed.ICEBERG} with the
 * BigQuery Lakehouse Iceberg REST catalog, using the {@code bq://} warehouse that surfaces BigQuery
 * datasets as namespaces.
 *
 * <p>Shares the {@code beam.bq.imt.*} system properties with BigQueryIOIcebergManagedTableIT in the
 * google-cloud-platform module.
 */
@RunWith(JUnit4.class)
public class BigQueryManagedTableCrossEngineIT {

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("BigQueryManagedTableCrossEngineIT");
  // BigqueryClient's query helpers stage a destination table (DDL rejects that) and run in the
  // default location, so SQL goes through a raw client instead.
  private static final Bigquery RAW_BQ =
      BigqueryClient.getNewBigqueryClient("BigQueryManagedTableCrossEngineIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

  // Connection in "projects/{project}/locations/{location}/connections/{connection}" form.
  private static final String CONNECTION =
      System.getProperty(
          "beam.bq.imt.connection",
          "projects/apache-beam-testing/locations/us/connections/apache-beam-testing-storageapi-biglake-nodelete");
  private static final String STORAGE_URI_ROOT =
      System.getProperty("beam.bq.imt.storageUri", "gs://apache-beam-testing-bq-biglake")
          + "/BigQueryManagedTableCrossEngineIT";

  private static final String DATASET_ID = "bq_imt_xengine_" + System.nanoTime();

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    // The dataset must be colocated with the connection.
    BQ_CLIENT.createNewDataset(
        PROJECT, DATASET_ID, /* defaultTableExpirationMs= */ null, connectionLocation());
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(PROJECT, DATASET_ID);
  }

  private static String connectionLocation() {
    return Splitter.on('/').splitToList(CONNECTION).get(3);
  }

  /** Connection reference in the dotted form BigQuery DDL accepts. */
  private static String connectionDotted() {
    List<String> parts = Splitter.on('/').splitToList(CONNECTION);
    return String.format("%s.%s.%s", parts.get(1), parts.get(3), parts.get(5));
  }

  /** Runs SQL in the connection's location. */
  private static void runSql(String sql) throws IOException {
    QueryResponse response =
        RAW_BQ
            .jobs()
            .query(
                PROJECT,
                new QueryRequest()
                    .setQuery(sql)
                    .setUseLegacySql(false)
                    .setLocation(connectionLocation())
                    .setTimeoutMs(180_000L))
            .execute();
    if (!Boolean.TRUE.equals(response.getJobComplete())) {
      throw new IOException("Query did not complete in time: " + sql);
    }
  }

  @Test
  public void testManagedIcebergReadByThreePartName() throws IOException {
    String table = "managed_read_" + System.nanoTime();
    runSql(
        String.format(
            "CREATE TABLE `%s.%s.%s` (id INT64, name STRING) WITH CONNECTION `%s` "
                + "OPTIONS (file_format='PARQUET', table_format='ICEBERG', storage_uri='%s/%s/%s')",
            PROJECT, DATASET_ID, table, connectionDotted(), STORAGE_URI_ROOT, DATASET_ID, table));
    runSql(
        String.format(
            "INSERT INTO `%s.%s.%s` "
                + "SELECT id, CONCAT('row_', CAST(id AS STRING)) "
                + "FROM UNNEST(GENERATE_ARRAY(0, 9)) id",
            PROJECT, DATASET_ID, table));
    // The catalog resolves the table through its exported Iceberg metadata, and automatic exports
    // can lag far behind recent writes; exporting makes the test deterministic.
    runSql(String.format("EXPORT TABLE METADATA FROM `%s.%s.%s`", PROJECT, DATASET_ID, table));

    Map<String, Object> config =
        ImmutableMap.<String, Object>builder()
            .put("table", DATASET_ID + "." + table)
            .put(
                "catalog_properties",
                ImmutableMap.<String, String>builder()
                    .put("type", "rest")
                    .put("uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
                    .put(
                        "warehouse",
                        String.format(
                            "bq://projects/%s/locations/%s", PROJECT, connectionLocation()))
                    .put("header.x-goog-user-project", PROJECT)
                    .put("rest-metrics-reporting-enabled", "false")
                    .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                    .put("rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
                    .build())
            .build();

    List<String> expected =
        LongStream.range(0, 10).mapToObj(i -> i + "|row_" + i).collect(Collectors.toList());

    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    PCollection<String> icebergRead =
        p.apply("Iceberg read", Managed.read(Managed.ICEBERG).withConfig(config))
            .getSinglePCollection()
            .apply(
                "canonicalize iceberg",
                MapElements.into(TypeDescriptors.strings())
                    .via(row -> row.getInt64("id") + "|" + row.getString("name")));
    PAssert.that(icebergRead).containsInAnyOrder(expected);

    // The same table read through the BigQuery lens, addressed project.dataset.table.
    PCollection<String> bigQueryRead =
        p.apply(
                "BigQuery read",
                Managed.read(Managed.BIGQUERY)
                    .withConfig(
                        ImmutableMap.of(
                            "table", String.format("%s.%s.%s", PROJECT, DATASET_ID, table))))
            .getSinglePCollection()
            .apply(
                "canonicalize bigquery",
                MapElements.into(TypeDescriptors.strings())
                    .via(row -> row.getInt64("id") + "|" + row.getString("name")));
    PAssert.that(bigQueryRead).containsInAnyOrder(expected);

    p.run().waitUntilFinish();
  }
}
