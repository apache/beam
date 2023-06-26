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
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for upsert and delete support. */
@RunWith(JUnit4.class)
public class StorageApiSinkRowUpdateIT {
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("StorageApiSinkRowUpdateIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_rows_update" + System.nanoTime();

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  private static String createTable(TableSchema tableSchema, List<String> primaryKey)
      throws IOException, InterruptedException {
    String table = "table" + System.nanoTime();

    BQ_CLIENT.deleteTable(PROJECT, BIG_QUERY_DATASET_ID, table);

    StringBuilder ddl =
        new StringBuilder("CREATE TABLE ")
            .append(PROJECT)
            .append(".")
            .append(BIG_QUERY_DATASET_ID)
            .append(".")
            .append(table)
            .append("(");
    for (TableFieldSchema tableFieldSchema : tableSchema.getFields()) {
      ddl.append(tableFieldSchema.getName())
          .append(" ")
          .append(tableFieldSchema.getType())
          .append(",");
    }

    String primaryKeyString = String.join(",", primaryKey);
    ddl.append(" PRIMARY KEY ")
        .append("(")
        .append(primaryKeyString)
        .append(")")
        .append(" NOT ENFORCED) ");
    ddl.append("CLUSTER BY ").append(primaryKeyString);

    BQ_CLIENT.queryWithRetriesUsingStandardSql(ddl.toString(), PROJECT);

    return PROJECT + "." + BIG_QUERY_DATASET_ID + "." + table;
  }

  @Test
  public void testCdc() throws Exception {
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("key1").setType("STRING"),
                    new TableFieldSchema().setName("key2").setType("STRING"),
                    new TableFieldSchema().setName("value").setType("STRING")));

    List<RowMutation> items =
        Lists.newArrayList(
            RowMutation.of(
                new TableRow().set("key1", "foo0").set("key2", "bar0").set("value", "1"),
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 0)),
            RowMutation.of(
                new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "1"),
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 0)),
            RowMutation.of(
                new TableRow().set("key1", "foo0").set("key2", "bar0").set("value", "2"),
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 1)),
            RowMutation.of(
                new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "1"),
                RowMutationInformation.of(RowMutationInformation.MutationType.DELETE, 1)),
            RowMutation.of(
                new TableRow().set("key1", "foo3").set("key2", "bar3").set("value", "1"),
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 0)),
            RowMutation.of(
                new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "3"),
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 2)),
            RowMutation.of(
                new TableRow().set("key1", "foo4").set("key2", "bar4").set("value", "1"),
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 0)),
            RowMutation.of(
                new TableRow().set("key1", "foo4").set("key2", "bar4").set("value", "1"),
                RowMutationInformation.of(RowMutationInformation.MutationType.DELETE, 1)));

    String tableSpec = createTable(tableSchema, Lists.newArrayList("key1", "key2"));
    Pipeline p = Pipeline.create();
    p.apply("Create rows", Create.of(items))
        .apply(
            "Apply updates",
            BigQueryIO.applyRowMutations()
                .to(tableSpec)
                .withSchema(tableSchema)
                .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    p.run();

    List<TableRow> expected =
        Lists.newArrayList(
            new TableRow().set("key1", "foo0").set("key2", "bar0").set("value", "2"),
            new TableRow().set("key1", "foo1").set("key2", "bar1").set("value", "3"),
            new TableRow().set("key1", "foo3").set("key2", "bar3").set("value", "1"));
    assertRowsWritten(tableSpec, expected);
  }

  private void assertRowsWritten(String tableSpec, Iterable<TableRow> expected)
      throws IOException, InterruptedException {
    List<TableRow> queryResponse =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM %s", tableSpec), PROJECT, true, true);
    assertThat(queryResponse, containsInAnyOrder(Iterables.toArray(expected, TableRow.class)));
  }
}
