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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for table create-if-needed handling when using the storage API. */
@RunWith(Parameterized.class)
public class StorageApiSinkCreateIfNeededIT {
  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {true}, new Object[] {false});
  }

  @Parameterized.Parameter(0)
  public boolean useAtLeastOnce;

  private static final Logger LOG = LoggerFactory.getLogger(StorageApiSinkCreateIfNeededIT.class);

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("StorageApiSinkFailedRowsIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_failed_rows" + System.nanoTime();
  private static final List<TableFieldSchema> FIELDS =
      ImmutableList.<TableFieldSchema>builder()
          .add(new TableFieldSchema().setType("STRING").setName("str"))
          .add(new TableFieldSchema().setType("INT64").setName("tablenum"))
          .build();
  private static final TableSchema BASE_TABLE_SCHEMA = new TableSchema().setFields(FIELDS);

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
  public void testCreateManyTables() throws IOException, InterruptedException {
    List<TableRow> inputs =
        LongStream.range(0, 100)
            .mapToObj(l -> new TableRow().set("str", "foo").set("tablenum", l))
            .collect(Collectors.toList());

    String table = "table" + System.nanoTime();
    String tableSpecBase = PROJECT + "." + BIG_QUERY_DATASET_ID + "." + table;
    runPipeline(getMethod(), tableSpecBase, inputs);
    assertTablesCreated(tableSpecBase, 100);
  }

  private void assertTablesCreated(String tableSpecPrefix, int expectedRows)
      throws IOException, InterruptedException {
    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(*) FROM `%s`", tableSpecPrefix + "*"),
                PROJECT,
                true,
                true));
    int numRowsWritten = Integer.parseInt((String) queryResponse.get("f0_"));
    if (useAtLeastOnce) {
      assertThat(numRowsWritten, Matchers.greaterThanOrEqualTo(expectedRows));
    } else {
      assertThat(numRowsWritten, Matchers.equalTo(expectedRows));
    }
  }

  private static void runPipeline(
      BigQueryIO.Write.Method method, String tableSpecBase, Iterable<TableRow> tableRows) {
    Pipeline p = Pipeline.create();

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tr -> new TableDestination(tableSpecBase + tr.getValue().get("tablenum"), ""))
            .withSchema(BASE_TABLE_SCHEMA)
            .withMethod(method)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
    if (method == BigQueryIO.Write.Method.STORAGE_WRITE_API) {
      write = write.withNumStorageWriteApiStreams(1);
      write = write.withTriggeringFrequency(Duration.standardSeconds(1));
    }
    PCollection<TableRow> input = p.apply("Create test cases", Create.of(tableRows));
    input = input.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    input.apply("Write using Storage Write API", write);

    p.run().waitUntilFinish();
  }
}
