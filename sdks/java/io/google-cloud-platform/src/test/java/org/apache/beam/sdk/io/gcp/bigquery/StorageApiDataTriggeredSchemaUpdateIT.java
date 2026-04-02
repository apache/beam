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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for BigQueryIO Storage Write API auto schema upgrade triggered by data.
 *
 * <p>Uses a Stateful DoFn to sequence elements ensuring base schema elements are written before
 * evolved schema elements, avoiding race conditions in distributed execution.
 */
@RunWith(JUnit4.class)
public class StorageApiDataTriggeredSchemaUpdateIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(StorageApiDataTriggeredSchemaUpdateIT.class);

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("StorageApiDataTriggeredSchemaUpdateIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_data_triggered_schema_update_" + System.nanoTime();

  private static String bigQueryLocation;

  @Rule public TestName testName = new TestName();

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    bigQueryLocation =
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class).getBigQueryLocation();
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID, null, bigQueryLocation);
  }

  @AfterClass
  public static void cleanUp() {
    LOG.info("Cleaning up dataset {} and tables.", BIG_QUERY_DATASET_ID);
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  private String createTable(TableSchema tableSchema) throws IOException, InterruptedException {
    String tableId = testName.getMethodName().replace("[", "_").replace("]", "_");
    BQ_CLIENT.deleteTable(PROJECT, BIG_QUERY_DATASET_ID, tableId);
    BQ_CLIENT.createNewTable(
        PROJECT,
        BIG_QUERY_DATASET_ID,
        new Table()
            .setSchema(tableSchema)
            .setTableReference(
                new TableReference()
                    .setTableId(tableId)
                    .setDatasetId(BIG_QUERY_DATASET_ID)
                    .setProjectId(PROJECT)));
    return tableId;
  }

  static class SequenceRowsDoFn extends DoFn<KV<Integer, Integer>, TableRow> {
    private static final String COUNTER = "counter";

    @StateId(COUNTER)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Integer>> counterSpec = StateSpecs.value();

    private final int stride;

    public SequenceRowsDoFn(int stride) {
      this.stride = stride;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(COUNTER) ValueState<Integer> counter) {
      int current = firstNonNull(counter.read(), 0);
      counter.write(++current);
      c.output(getRow(current));
    }

    TableRow getRow(int i) {
      TableRow row = new TableRow().set("name", "name" + i).set("number", Long.toString(i));
      if (i < stride) {
        row = row.set("req", "foo");
      } else {
        row = row.set("new1", "blah" + i);
        row = row.set("new2", "baz" + i);

        if (i >= 2 * stride) {
          TableRow nested =
              new TableRow()
                  .set("nested_field1", "nested1" + i)
                  .set("nested_field2", "nested2" + i);

          if (i >= 3 * stride) {
            TableRow double_nested =
                new TableRow().set("double_nested_field1", "double_nested1" + i);
            nested = nested.set("double_nested", double_nested);

            // Add a repeated struct to ensure that we capture this code path as well.
            TableRow repeated_nested1 =
                new TableRow().set("repeated_nested_field1", "repeated_nested1" + i);
            TableRow repeated_nested2 =
                new TableRow().set("repeated_nested_field2", "repeated_nested2" + i);
            nested =
                nested.set("repeated_nested", ImmutableList.of(repeated_nested1, repeated_nested2));
          }
          row.set("nested", nested);
        }
      }
      return row;
    };
  }

  @Test
  public void testDataTriggeredSchemaUpgradeExactlyOnce() throws Exception {
    runTest(Write.Method.STORAGE_WRITE_API);
  }

  @Test
  public void testDataTriggeredSchemaUpgradeAtLeastOnce() throws Exception {
    runTest(Write.Method.STORAGE_API_AT_LEAST_ONCE);
  }

  private void runTest(Write.Method method) throws Exception {
    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    p.getOptions().as(BigQueryOptions.class).setSchemaUpgradeBufferingShards(1);

    TableSchema baseSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INT64"),
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("req").setType("STRING").setMode("REQUIRED")));

    String tableId = createTable(baseSchema);
    String tableSpec = PROJECT + ":" + BIG_QUERY_DATASET_ID + "." + tableId;

    List<Integer> dummyInputs = IntStream.range(0, 20).boxed().collect(Collectors.toList());

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withMethod(method)
            .withSchemaUpdateOptions(
                ImmutableSet.of(
                    Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                    Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION))
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    if (method == Write.Method.STORAGE_WRITE_API) {
      write =
          write
              .withTriggeringFrequency(Duration.standardSeconds(1))
              .withNumStorageWriteApiStreams(2);
    }

    p.apply("Create Dummy Inputs", Create.of(dummyInputs))
        .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
        .apply("Add a dummy key", WithKeys.of(1))
        .apply("Sequence Rows", ParDo.of(new SequenceRowsDoFn(5)))
        .apply("Stream to BigQuery", write);

    p.run().waitUntilFinish();

    // Verification
    verifyTableSchemaUpdated(tableSpec);
    //    verifyDataWritten(tableSpec, 10, "age");
  }

  private void verifyTableSchemaUpdated(String tableSpec) throws IOException, InterruptedException {
    Table table =
        BQ_CLIENT.getTableResource(
            PROJECT,
            BIG_QUERY_DATASET_ID,
            Iterables.getLast(
                org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter.on('.')
                    .split(tableSpec)));
    System.err.println("NEW SCHEMA " + table.getSchema());
    /*
    boolean foundAge = false;
    for (TableFieldSchema field : table.getSchema().getFields()) {
      if (field.getName().equals(extraField)) {
        foundAge = true;
        break;
      }
    }
    assertTrue("Table schema should include the upgraded field '" + extraField + "'", foundAge);
     */
  }

  private void verifyDataWritten(String tableSpec, int expectedTotalRows, String extraField)
      throws IOException, InterruptedException {
    TableRow countResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(1) as total FROM [%s]", tableSpec),
                PROJECT,
                true,
                false,
                bigQueryLocation));
    assertEquals(expectedTotalRows, Integer.parseInt((String) countResponse.get("total")));

    List<TableRow> rows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM [%s] WHERE age IS NOT NULL", tableSpec),
            PROJECT,
            true,
            false,
            bigQueryLocation);
    // We added 'age' to rows with id >= 5 (indices 5 to 9)
    assertEquals(5, rows.size());
    for (TableRow row : rows) {
      int id = Integer.parseInt((String) row.get("id"));
      assertTrue("Row ID should be >= 5 for rows with age", id >= 5);
      assertNotNull("Age should be present", row.get(extraField));
    }
  }
}
