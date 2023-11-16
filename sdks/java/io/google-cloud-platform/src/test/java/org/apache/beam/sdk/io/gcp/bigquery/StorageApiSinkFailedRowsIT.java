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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for failed-rows handling when using the storage API. */
@RunWith(Parameterized.class)
public class StorageApiSinkFailedRowsIT {
  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(
        new Object[] {true, false, false},
        new Object[] {false, true, false},
        new Object[] {false, false, true},
        new Object[] {true, false, true});
  }

  @Parameterized.Parameter(0)
  public boolean useStreamingExactlyOnce;

  @Parameterized.Parameter(1)
  public boolean useAtLeastOnce;

  @Parameterized.Parameter(2)
  public boolean useBatch;

  private static final Logger LOG = LoggerFactory.getLogger(StorageApiSinkFailedRowsIT.class);
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("StorageApiSinkFailedRowsIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_failed_rows" + System.nanoTime();

  private static final List<TableFieldSchema> FIELDS =
      ImmutableList.<TableFieldSchema>builder()
          .add(new TableFieldSchema().setType("STRING").setName("str"))
          .add(new TableFieldSchema().setType("INT64").setName("i64"))
          .add(new TableFieldSchema().setType("DATE").setName("date"))
          .add(new TableFieldSchema().setType("STRING").setMaxLength(1L).setName("strone"))
          .add(new TableFieldSchema().setType("BYTES").setName("bytes"))
          .add(new TableFieldSchema().setType("JSON").setName("json"))
          .add(
              new TableFieldSchema()
                  .setType("STRING")
                  .setMaxLength(1L)
                  .setMode("REPEATED")
                  .setName("stronearray"))
          .build();

  private static final TableSchema BASE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .addAll(FIELDS)
                  .add(new TableFieldSchema().setType("STRUCT").setFields(FIELDS).setName("inner"))
                  .build());

  private static final byte[] BIG_BYTES = new byte[11 * 1024 * 1024];

  private BigQueryIO.Write.Method getMethod() {
    return useAtLeastOnce
        ? BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE
        : BigQueryIO.Write.Method.STORAGE_WRITE_API;
  }

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    LOG.info("Start to clean up tables and datasets.");
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @Test
  public void testSchemaMismatchCaughtByBeam() throws IOException, InterruptedException {
    String tableSpec = createTable(BASE_TABLE_SCHEMA);
    TableRow good1 = new TableRow().set("str", "foo").set("i64", "42");
    TableRow good2 = new TableRow().set("str", "foo").set("i64", "43");
    Iterable<TableRow> goodRows =
        ImmutableList.of(
            good1.clone().set("inner", new TableRow()),
            good2.clone().set("inner", new TableRow()),
            new TableRow().set("inner", good1),
            new TableRow().set("inner", good2));

    TableRow bad1 = new TableRow().set("str", "foo").set("i64", "baad");
    TableRow bad2 = new TableRow().set("str", "foo").set("i64", "42").set("unknown", "foobar");
    Iterable<TableRow> badRows =
        ImmutableList.of(
            bad1, bad2, new TableRow().set("inner", bad1), new TableRow().set("inner", bad2));

    runPipeline(
        getMethod(),
        useStreamingExactlyOnce,
        tableSpec,
        Iterables.concat(goodRows, badRows),
        badRows);
    assertGoodRowsWritten(tableSpec, goodRows);
  }

  @Test
  public void testInvalidRowCaughtByBigquery() throws IOException, InterruptedException {
    String tableSpec = createTable(BASE_TABLE_SCHEMA);

    TableRow good1 =
        new TableRow()
            .set("str", "foo")
            .set("i64", "42")
            .set("date", "2022-08-16")
            .set("stronearray", Lists.newArrayList());
    TableRow good2 =
        new TableRow().set("str", "foo").set("i64", "43").set("stronearray", Lists.newArrayList());
    Iterable<TableRow> goodRows =
        ImmutableList.of(
            good1.clone().set("inner", new TableRow().set("stronearray", Lists.newArrayList())),
            good2.clone().set("inner", new TableRow().set("stronearray", Lists.newArrayList())),
            new TableRow().set("inner", good1).set("stronearray", Lists.newArrayList()),
            new TableRow().set("inner", good2).set("stronearray", Lists.newArrayList()));

    TableRow bad1 = new TableRow().set("str", "foo").set("i64", "42").set("date", "10001-08-16");
    TableRow bad2 = new TableRow().set("str", "foo").set("i64", "42").set("strone", "ab");
    TableRow bad3 = new TableRow().set("str", "foo").set("i64", "42").set("json", "BAADF00D");
    TableRow bad4 =
        new TableRow()
            .set("str", "foo")
            .set("i64", "42")
            .set("stronearray", Lists.newArrayList("toolong"));
    TableRow bad5 = new TableRow().set("bytes", BIG_BYTES);
    Iterable<TableRow> badRows =
        ImmutableList.of(
            bad1,
            bad2,
            bad3,
            bad4,
            bad5,
            new TableRow().set("inner", bad1),
            new TableRow().set("inner", bad2),
            new TableRow().set("inner", bad3));

    runPipeline(
        getMethod(),
        useStreamingExactlyOnce,
        tableSpec,
        Iterables.concat(goodRows, badRows),
        badRows);
    assertGoodRowsWritten(tableSpec, goodRows);
  }

  private static String createTable(TableSchema tableSchema)
      throws IOException, InterruptedException {
    String table = "table" + System.nanoTime();
    BQ_CLIENT.deleteTable(PROJECT, BIG_QUERY_DATASET_ID, table);
    BQ_CLIENT.createNewTable(
        PROJECT,
        BIG_QUERY_DATASET_ID,
        new Table()
            .setSchema(tableSchema)
            .setTableReference(
                new TableReference()
                    .setTableId(table)
                    .setDatasetId(BIG_QUERY_DATASET_ID)
                    .setProjectId(PROJECT)));
    return PROJECT + "." + BIG_QUERY_DATASET_ID + "." + table;
  }

  private void assertGoodRowsWritten(String tableSpec, Iterable<TableRow> goodRows)
      throws IOException, InterruptedException {
    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(*) FROM %s", tableSpec), PROJECT, true, true));
    int numRowsWritten = Integer.parseInt((String) queryResponse.get("f0_"));
    if (useAtLeastOnce) {
      assertThat(numRowsWritten, Matchers.greaterThanOrEqualTo(Iterables.size(goodRows)));
    } else {
      assertThat(numRowsWritten, Matchers.equalTo(Iterables.size(goodRows)));
    }
  }

  private static void runPipeline(
      BigQueryIO.Write.Method method,
      boolean triggered,
      String tableSpec,
      Iterable<TableRow> tableRows,
      Iterable<TableRow> expectedFailedRows) {
    Pipeline p = Pipeline.create();

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withSchema(BASE_TABLE_SCHEMA)
            .withMethod(method)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER);
    if (method == BigQueryIO.Write.Method.STORAGE_WRITE_API) {
      write = write.withNumStorageWriteApiStreams(1);
      if (triggered) {
        write = write.withTriggeringFrequency(Duration.standardSeconds(1));
      }
    }
    PCollection<TableRow> input = p.apply("Create test cases", Create.of(tableRows));
    if (triggered) {
      input = input.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    }
    WriteResult result = input.apply("Write using Storage Write API", write);

    PCollection<TableRow> failedRows =
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(BigQueryStorageApiInsertError::getRow));

    PAssert.that(failedRows).containsInAnyOrder(expectedFailedRows);

    p.run().waitUntilFinish();
  }
}
