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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for BigQueryIO against managed Apache Iceberg tables in BigQuery.
 *
 * <p>Requires a CLOUD_RESOURCE connection whose service account can administer the storage bucket.
 * Defaults target standing apache-beam-testing resources (shared with {@link
 * StorageApiSinkCreateIfNeededIT}); override with the {@code beam.bq.imt.connection} and {@code
 * beam.bq.imt.storageUri} system properties.
 */
@RunWith(JUnit4.class)
public class BigQueryIOIcebergManagedTableIT {

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("BigQueryIOIcebergManagedTableIT");
  // BigqueryClient's query helpers stage a destination table (DDL rejects that) and run in the
  // default location, so SQL goes through a raw client instead.
  private static final Bigquery RAW_BQ =
      BigqueryClient.getNewBigqueryClient("BigQueryIOIcebergManagedTableIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

  // Connection in "projects/{project}/locations/{location}/connections/{connection}" form.
  private static final String CONNECTION =
      System.getProperty(
          "beam.bq.imt.connection",
          "projects/apache-beam-testing/locations/us/connections/apache-beam-testing-storageapi-biglake-nodelete");
  private static final String STORAGE_URI_ROOT =
      System.getProperty("beam.bq.imt.storageUri", "gs://apache-beam-testing-bq-biglake")
          + "/BigQueryIOIcebergManagedTableIT";

  private static final String DATASET_ID = "bq_imt_it_" + System.nanoTime();

  private static final TableSchema BASE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("id").setType("INT64").setMode("REQUIRED"),
                  new TableFieldSchema().setName("name").setType("STRING")));

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

  private static String tableSpec(String table) {
    return String.format("%s.%s.%s", PROJECT, DATASET_ID, table);
  }

  private static List<TableRow> rows(long from, long to) {
    return LongStream.range(from, to)
        .mapToObj(i -> new TableRow().set("id", i).set("name", "row_" + i))
        .collect(Collectors.toList());
  }

  private static String canonical(TableRow row) {
    return row.get("id") + "|" + row.get("name");
  }

  private static List<String> canonical(List<TableRow> rows) {
    return rows.stream()
        .map(BigQueryIOIcebergManagedTableIT::canonical)
        .collect(Collectors.toList());
  }

  private ImmutableMap<String, String> bigLakeConfig() {
    return ImmutableMap.of(
        BigQueryIO.CONNECTION_ID, CONNECTION,
        BigQueryIO.STORAGE_URI, STORAGE_URI_ROOT);
  }

  /** Runs SQL in the connection's location and returns the result rows. */
  private static List<TableRow> runSql(String sql) throws IOException {
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
    return response.getRows();
  }

  private static String firstCell(List<TableRow> rows) {
    TableCell cell = (TableCell) Iterables.getOnlyElement(rows).getF().get(0);
    return (String) cell.getV();
  }

  private void createManagedTableViaDdl(String table, String columns) throws IOException {
    runSql(
        String.format(
            "CREATE TABLE `%s` (%s) WITH CONNECTION `%s` "
                + "OPTIONS (file_format='PARQUET', table_format='ICEBERG', storage_uri='%s/%s/%s')",
            tableSpec(table), columns, connectionDotted(), STORAGE_URI_ROOT, DATASET_ID, table));
  }

  private void runWrite(
      BigQueryIO.Write.Method method,
      String table,
      List<TableRow> input,
      BigQueryIO.Write.CreateDisposition createDisposition,
      @Nullable TableSchema schema,
      boolean withBigLakeConfiguration) {
    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec(table))
            .withMethod(method)
            .withCreateDisposition(createDisposition)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    if (schema != null) {
      write = write.withSchema(schema);
    }
    if (withBigLakeConfiguration) {
      write = write.withBigLakeConfiguration(bigLakeConfig());
    }
    p.apply(Create.of(input).withCoder(TableRowJsonCoder.of())).apply(write);
    p.run().waitUntilFinish();
  }

  private long countRows(String table) throws IOException {
    return Long.parseLong(
        firstCell(runSql(String.format("SELECT COUNT(*) FROM `%s`", tableSpec(table)))));
  }

  @Test
  public void testAtLeastOnceWriteThenDirectRead() throws IOException, InterruptedException {
    String table = "alo_create_" + System.nanoTime();
    List<TableRow> input = rows(0, 20);
    runWrite(
        BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE,
        table,
        input,
        BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED,
        BASE_SCHEMA,
        /* withBigLakeConfiguration= */ true);

    Table created = BQ_CLIENT.getTableResource(PROJECT, DATASET_ID, table);
    assertNotNull(created.getBiglakeConfiguration());
    assertEquals("ICEBERG", created.getBiglakeConfiguration().getTableFormat());
    // Stats can lag behind recent writes and report zero, but are never null for these tables.
    assertNotNull(created.getNumBytes());

    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    PCollection<String> full =
        p.apply(
                "ReadFull",
                BigQueryIO.readTableRows().from(tableSpec(table)).withMethod(Method.DIRECT_READ))
            .apply(
                "CanonicalFull",
                MapElements.into(TypeDescriptors.strings())
                    .via(BigQueryIOIcebergManagedTableIT::canonical));
    PAssert.that(full).containsInAnyOrder(canonical(input));

    PCollection<String> pushdown =
        p.apply(
                "ReadPushdown",
                BigQueryIO.readTableRows()
                    .from(tableSpec(table))
                    .withMethod(Method.DIRECT_READ)
                    .withSelectedFields(ImmutableList.of("id"))
                    .withRowRestriction("id < 5"))
            .apply(
                "CanonicalPushdown",
                MapElements.into(TypeDescriptors.strings()).via(r -> String.valueOf(r.get("id"))));
    PAssert.that(pushdown).containsInAnyOrder("0", "1", "2", "3", "4");
    p.run().waitUntilFinish();
  }

  /** Exactly-once batch write: bounded STORAGE_WRITE_API (PENDING streams + batch commit). */
  @Test
  public void testExactlyOnceBatchWrite() throws IOException, InterruptedException {
    String table = "eo_batch_" + System.nanoTime();
    runWrite(
        BigQueryIO.Write.Method.STORAGE_WRITE_API,
        table,
        rows(0, 50),
        BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED,
        BASE_SCHEMA,
        /* withBigLakeConfiguration= */ true);
    assertEquals(50L, countRows(table));
  }

  /** Exactly-once streaming write: unbounded STORAGE_WRITE_API with triggering frequency. */
  @Test
  public void testExactlyOnceStreamingWrite() throws IOException, InterruptedException {
    String table = "eo_streaming_" + System.nanoTime();
    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    // The streaming exactly-once path is chosen by input boundedness; flip the bounded Create.
    p.apply(Create.of(rows(0, 30)).withCoder(TableRowJsonCoder.of()))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
        .apply(
            BigQueryIO.writeTableRows()
                .to(tableSpec(table))
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withSchema(BASE_SCHEMA)
                .withBigLakeConfiguration(bigLakeConfig())
                .withNumStorageWriteApiStreams(1)
                .withTriggeringFrequency(Duration.standardSeconds(1)));
    p.run().waitUntilFinish();
    assertEquals(30L, countRows(table));
  }

  /** The common production case: CREATE_NEVER write into a pre-existing managed Iceberg table. */
  @Test
  public void testCreateNeverToPreExistingTable() throws IOException, InterruptedException {
    String table = "create_never_" + System.nanoTime();
    createManagedTableViaDdl(table, "id INT64 NOT NULL, name STRING");
    runWrite(
        BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE,
        table,
        rows(0, 10),
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER,
        null,
        /* withBigLakeConfiguration= */ false);
    assertThat(countRows(table), Matchers.greaterThanOrEqualTo(10L));
  }

  /**
   * Schema widening on a managed Iceberg table: tables.patch accepts a new nullable column (unlike
   * Iceberg tables in catalogs, which refuse patch), and subsequent writes can use it.
   */
  @Test
  public void testSchemaFieldAdditionThenWrite() throws IOException, InterruptedException {
    String table = "schema_update_" + System.nanoTime();
    createManagedTableViaDdl(table, "id INT64 NOT NULL, name STRING");

    TableSchema widened =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("id").setType("INT64").setMode("REQUIRED"),
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("extra").setType("STRING")));
    BQ_CLIENT.updateTableSchema(PROJECT, DATASET_ID, table, widened);

    List<TableRow> input =
        ImmutableList.of(
            new TableRow().set("id", 1L).set("name", "a").set("extra", "x"),
            new TableRow().set("id", 2L).set("name", "b").set("extra", "y"));
    runWrite(
        BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE,
        table,
        input,
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER,
        null,
        /* withBigLakeConfiguration= */ false);

    long withExtra =
        Long.parseLong(
            firstCell(
                runSql(
                    String.format(
                        "SELECT COUNT(*) FROM `%s` WHERE extra IS NOT NULL", tableSpec(table)))));
    assertThat(withExtra, Matchers.greaterThanOrEqualTo(2L));
  }

  /**
   * FILE_LOADS append into a pre-existing table. withBigLakeConfiguration is rejected outside the
   * Storage Write API, so FILE_LOADS cannot create these tables itself.
   */
  @Test
  public void testFileLoadsSinglePartition() throws IOException, InterruptedException {
    String table = "file_loads_" + System.nanoTime();
    createManagedTableViaDdl(table, "id INT64 NOT NULL, name STRING");
    runWrite(
        BigQueryIO.Write.Method.FILE_LOADS,
        table,
        rows(0, 25),
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER,
        BASE_SCHEMA,
        /* withBigLakeConfiguration= */ false);
    assertEquals(25L, countRows(table));
  }

  /** Legacy streaming (insertAll) is accepted; rows arrive through the streaming buffer. */
  @Test
  public void testStreamingInsertsWrite() throws IOException, InterruptedException {
    String table = "streaming_inserts_" + System.nanoTime();
    createManagedTableViaDdl(table, "id INT64 NOT NULL, name STRING");
    runWrite(
        BigQueryIO.Write.Method.STREAMING_INSERTS,
        table,
        rows(0, 5),
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER,
        BASE_SCHEMA,
        /* withBigLakeConfiguration= */ false);
    assertThat(countRows(table), Matchers.greaterThanOrEqualTo(5L));
  }

  /** Query read with time travel: managed Iceberg tables support FOR SYSTEM_TIME AS OF. */
  @Test
  public void testQueryReadWithTimeTravel() throws IOException, InterruptedException {
    String table = "time_travel_" + System.nanoTime();
    runWrite(
        BigQueryIO.Write.Method.STORAGE_WRITE_API,
        table,
        rows(0, 10),
        BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED,
        BASE_SCHEMA,
        /* withBigLakeConfiguration= */ true);
    // Server-side timestamp between the two writes.
    String asOf = firstCell(runSql("SELECT STRING(CURRENT_TIMESTAMP())"));
    runWrite(
        BigQueryIO.Write.Method.STORAGE_WRITE_API,
        table,
        rows(10, 20),
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER,
        null,
        /* withBigLakeConfiguration= */ false);

    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    PCollection<String> counted =
        p.apply(
                BigQueryIO.readTableRows()
                    .fromQuery(
                        String.format(
                            "SELECT COUNT(*) AS c FROM `%s` FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                            tableSpec(table), asOf))
                    .usingStandardSql())
            .apply(
                MapElements.into(TypeDescriptors.strings()).via(r -> String.valueOf(r.get("c"))));
    PAssert.that(counted).containsInAnyOrder("10");
    p.run().waitUntilFinish();
  }
}
