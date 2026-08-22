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
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
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
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
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
    private final int badRowIndex;

    public SequenceRowsDoFn(int stride, int badRowIndex) {
      this.stride = stride;
      this.badRowIndex = badRowIndex;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(COUNTER) ValueState<Integer> counter) {
      int current = firstNonNull(counter.read(), 0);
      c.output(getRow(current));
      counter.write(++current);
    }

    TableRow getRow(int i) {
      TableRow row = new TableRow().set("name", "name" + i).set("number", Long.toString(i));
      if (i < stride) {
        row = row.set("req", "42");
      } else {
        row = row.set("new1", "blah" + i);
        row = row.set("new2", "baz" + i);

        if (i >= 2 * stride) {
          TableRow nested =
              new TableRow()
                  .set("nested_field1", "nested1" + i)
                  .set("nested_field2", "nested2" + i);

          if (i >= 3 * stride) {
            TableRow doubleNested =
                new TableRow().set("double_nested_field1", "double_nested1" + i);
            nested = nested.set("double_nested", doubleNested);

            // Add a repeated struct to ensure that we capture this code path as well.
            TableRow repeatedNested1 =
                new TableRow().set("repeated_nested_field1", "repeated_nested1" + i);
            TableRow repeatedNested2 =
                new TableRow().set("repeated_nested_field2", "repeated_nested2" + i);
            nested =
                nested.set("repeated_nested", ImmutableList.of(repeatedNested1, repeatedNested2));
          }
          row.set("nested", nested);
        }
      }

      if (i == badRowIndex) {
        row.set("req", ImmutableList.of("43", "44"));
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
    p.getOptions().as(StreamingOptions.class).setStreaming(true);

    TableSchema baseSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INT64"),
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("req").setType("INT64").setMode("REQUIRED")));
    TableSchema evolvedSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INT64").setMode("NULLABLE"),
                    new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("req").setType("INT64").setMode("NULLABLE"),
                    new TableFieldSchema().setName("new1").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("new2").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema()
                        .setName("nested")
                        .setType("STRUCT")
                        .setMode("NULLABLE")
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema()
                                    .setName("nested_field1")
                                    .setType("STRING")
                                    .setMode("NULLABLE"),
                                new TableFieldSchema()
                                    .setName("nested_field2")
                                    .setType("STRING")
                                    .setMode("NULLABLE"),
                                new TableFieldSchema()
                                    .setName("double_nested")
                                    .setType("STRUCT")
                                    .setMode("NULLABLE")
                                    .setFields(
                                        ImmutableList.of(
                                            new TableFieldSchema()
                                                .setName("double_nested_field1")
                                                .setType("STRING")
                                                .setMode("NULLABLE"))),
                                new TableFieldSchema()
                                    .setName("repeated_nested")
                                    .setType("STRUCT")
                                    .setMode("REPEATED")
                                    .setFields(
                                        ImmutableList.of(
                                            new TableFieldSchema()
                                                .setName("repeated_nested_field1")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                            new TableFieldSchema()
                                                .setName("repeated_nested_field2")
                                                .setType("STRING")
                                                .setMode("NULLABLE")))))));

    String tableId = createTable(baseSchema);
    String tableSpec = PROJECT + ":" + BIG_QUERY_DATASET_ID + "." + tableId;

    List<Integer> dummyInputs = IntStream.range(0, 21).boxed().collect(Collectors.toList());

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
              // One stream — same as other Storage Write ITs here, fewer ordering surprises.
              .withNumStorageWriteApiStreams(1);
    }

    SequenceRowsDoFn doFn = new SequenceRowsDoFn(5, 20);
    WriteResult result =
        p.apply("Create Dummy Inputs", Create.of(dummyInputs))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
            .apply("Add a dummy key", WithKeys.of(1))
            .apply("Sequence Rows", ParDo.of(doFn))
            .apply("Stream to BigQuery", write);

    PCollection<TableRow> failedInserts =
        result
            .getFailedStorageApiInserts()
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(BigQueryStorageApiInsertError::getRow));
    // Schema upgrades can race with evolved rows; allow extra DLQ rows but require the
    // intentionally malformed row shape to appear.
    PAssert.that(failedInserts).satisfies(new VerifyContainsMalformedReqRow());

    p.run().waitUntilFinish();

    // Verification
    verifyTableSchemaUpdated(tableSpec, evolvedSchema);
    List<VerificationInfo> verifications =
        ImmutableList.of(
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo("", 20),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo("req IS NULL", 15),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "new1 IS NOT NULL", 15),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "new2 IS NOT NULL", 15),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "nested.nested_field1 IS NOT NULL", 10),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "nested.nested_field2 IS NOT NULL", 10),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "nested.double_nested.double_nested_field1 IS NOT NULL", 5),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "nested.repeated_nested.repeated_nested_field1 IS NOT NULL", 5),
            new AutoValue_StorageApiDataTriggeredSchemaUpdateIT_VerificationInfo(
                "nested.repeated_nested.repeated_nested_field2 IS NOT NULL", 5));
    verifyDataWritten(tableSpec, verifications);
  }

  private void verifyTableSchemaUpdated(String tableSpec, TableSchema evolvedSchema)
      throws IOException, InterruptedException {
    Table table =
        BQ_CLIENT.getTableResource(
            PROJECT,
            BIG_QUERY_DATASET_ID,
            Iterables.getLast(
                org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter.on('.')
                    .split(tableSpec)));
    assertEquals(
        TableRowToStorageApiProto.schemaToProtoTableSchema(evolvedSchema),
        TableRowToStorageApiProto.schemaToProtoTableSchema(table.getSchema()));
  }

  @AutoValue
  abstract static class VerificationInfo {
    abstract String getFilter();

    abstract int getExpectedCount();
  }

  private static final class VerifyContainsMalformedReqRow
      implements SerializableFunction<Iterable<TableRow>, Void> {
    @Override
    public Void apply(Iterable<TableRow> rows) {
      boolean sawBadReqShape = false;
      for (TableRow row : rows) {
        Object reqValue = row.get("req");
        if (reqValue instanceof List && ((List<?>) reqValue).size() == 2) {
          sawBadReqShape = true;
          break;
        }
      }
      assertTrue("DLQ should include the malformed req row", sawBadReqShape);
      return null;
    }
  }

  private void verifyDataWritten(String tableSpec, List<VerificationInfo> verifications)
      throws IOException, InterruptedException {
    for (VerificationInfo verification : verifications) {
      String format =
          verification.getFilter().isEmpty()
              ? String.format("SELECT COUNT(1) as total FROM [%s]", tableSpec)
              : String.format(
                  "SELECT COUNT(1) as total FROM [%s] WHERE %s",
                  tableSpec, verification.getFilter());
      TableRow totalCountResponse =
          Iterables.getOnlyElement(
              BQ_CLIENT.queryUnflattened(format, PROJECT, true, false, bigQueryLocation));
      assertEquals(
          "Unexpected result for query " + format,
          verification.getExpectedCount(),
          Integer.parseInt((String) totalCountResponse.get("total")));
    }
  }
}
