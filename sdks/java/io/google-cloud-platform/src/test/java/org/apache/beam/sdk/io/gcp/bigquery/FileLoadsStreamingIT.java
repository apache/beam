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
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class FileLoadsStreamingIT {
  private static final Logger LOG = LoggerFactory.getLogger(FileLoadsStreamingIT.class);

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {false}, new Object[] {true});
  }

  @Parameterized.Parameter(0)
  public boolean useInputSchema;

  @Rule public TestName testName = new TestName();

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("FileLoadsStreamingIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID = "file_loads_streaming_it_" + System.nanoTime();

  private static final String[] FIELDS = {
    "BOOL",
    "BOOLEAN",
    "BYTES",
    "INT64",
    "INTEGER",
    "FLOAT",
    "FLOAT64",
    "NUMERIC",
    "STRING",
    "DATE",
    "TIMESTAMP"
  };

  private static final int TOTAL_N = 50;

  private final Random randomGenerator = new Random();

  // used when test suite specifies a particular GCP location for BigQuery operations
  private static String bigQueryLocation;

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    cleanUp();
    bigQueryLocation =
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class).getBigQueryLocation();
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID, null, bigQueryLocation);
  }

  @AfterClass
  public static void cleanUp() {
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  static class GenerateRowFunc implements SerializableFunction<Long, TableRow> {
    private final List<String> fieldNames;

    public GenerateRowFunc(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public TableRow apply(Long rowId) {
      TableRow row = new TableRow();
      row.set("id", rowId);

      for (String name : fieldNames) {
        String type = Iterables.get(Splitter.on('_').split(name), 0);
        switch (type) {
          case "BOOL":
          case "BOOLEAN":
            if (rowId % 2 == 0) {
              row.set(name, false);
            } else {
              row.set(name, true);
            }
            break;
          case "BYTES":
            row.set(name, String.format("test_blob_%s", rowId).getBytes(StandardCharsets.UTF_8));
            break;
          case "INT64":
          case "INTEGER":
            row.set(name, String.valueOf(rowId + 10));
            break;
          case "FLOAT":
          case "FLOAT64":
            row.set(name, String.valueOf(0.5 + rowId));
            break;
          case "NUMERIC":
            row.set(name, String.valueOf(rowId + 0.12345));
            break;
          case "DATE":
            row.set(name, "2022-01-01");
            break;
          case "TIMESTAMP":
            row.set(name, "2022-01-01 10:10:10.012 UTC");
            break;
          case "STRING":
            row.set(name, "test_string" + rowId);
            break;
          default:
            row.set(name, "unknown" + rowId);
            break;
        }
      }
      return row;
    }
  }

  private static TableSchema makeTableSchemaFromTypes(List<String> fieldNames) {
    ImmutableList.Builder<TableFieldSchema> builder = ImmutableList.<TableFieldSchema>builder();

    // Add an id field for verification of correctness
    builder.add(new TableFieldSchema().setType("INTEGER").setName("id").setMode("REQUIRED"));

    // the name is prefix with type_.
    for (String name : fieldNames) {
      String mode = "REQUIRED";
      builder.add(new TableFieldSchema().setType(name).setName(name).setMode(mode));
    }

    return new TableSchema().setFields(builder.build());
  }

  private String maybeCreateTable(TableSchema tableSchema, String suffix)
      throws IOException, InterruptedException {
    String tableId = Iterables.get(Splitter.on('[').split(testName.getMethodName()), 0);

    BQ_CLIENT.deleteTable(PROJECT, BIG_QUERY_DATASET_ID, tableId + suffix);
    if (!useInputSchema) {
      BQ_CLIENT.createNewTable(
          PROJECT,
          BIG_QUERY_DATASET_ID,
          new Table()
              .setSchema(tableSchema)
              .setTableReference(
                  new TableReference()
                      .setTableId(tableId + suffix)
                      .setDatasetId(BIG_QUERY_DATASET_ID)
                      .setProjectId(PROJECT)));
    } else {
      tableId += "WithInputSchema";
    }
    return String.format("%s.%s.%s", PROJECT, BIG_QUERY_DATASET_ID, tableId + suffix);
  }

  private void runStreaming(int numFileShards, boolean useCopyJobs)
      throws IOException, InterruptedException {
    TestPipelineOptions opts = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    opts.setTempLocation(opts.getTempRoot());
    Pipeline p = Pipeline.create(opts);

    // Only run the most relevant test case on Dataflow.
    // Testing this dimension on DirectRunner is sufficient
    if (p.getOptions().getRunner().getName().contains("DataflowRunner")) {
      assumeTrue("Skipping in favor of more relevant test case", useInputSchema);
      // Need to manually enable streaming engine for legacy dataflow runner
      ExperimentalOptions.addExperiment(
          p.getOptions().as(ExperimentalOptions.class), GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    }

    List<String> fieldNamesOrigin = Arrays.asList(FIELDS);
    // Shuffle the fields in the write schema to do fuzz testing on field order
    List<String> fieldNamesShuffled = new ArrayList<String>(fieldNamesOrigin);
    Collections.shuffle(fieldNamesShuffled, randomGenerator);

    TableSchema bqTableSchema = makeTableSchemaFromTypes(fieldNamesOrigin);
    TableSchema inputSchema = makeTableSchemaFromTypes(fieldNamesShuffled);
    String tableSpec = maybeCreateTable(bqTableSchema, "");

    // set up and build pipeline
    Instant start = new Instant(0);
    GenerateRowFunc generateRowFunc = new GenerateRowFunc(fieldNamesShuffled);
    PCollection<Instant> instants =
        p.apply(
            "Generate Instants",
            PeriodicImpulse.create()
                .startAt(start)
                .stopAt(start.plus(Duration.standardSeconds(TOTAL_N - 1)))
                .withInterval(Duration.standardSeconds(1))
                .catchUpToNow(false));
    PCollection<TableRow> rows =
        instants.apply(
            "Create TableRows",
            MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(instant -> generateRowFunc.apply(instant.getMillis() / 1000)));
    // build write transform
    Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withMethod(Write.Method.FILE_LOADS)
            .withTriggeringFrequency(Duration.standardSeconds(10));
    if (useCopyJobs) {
      write = write.withMaxBytesPerPartition(250);
    }
    if (useInputSchema) {
      // we're creating the table with the input schema
      write =
          write
              .withSchema(inputSchema)
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    } else {
      // table already exists with a schema, no need to create it
      write =
          write
              .withCreateDisposition(CreateDisposition.CREATE_NEVER)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    }
    write = numFileShards == 0 ? write.withAutoSharding() : write.withNumFileShards(numFileShards);

    rows.apply("Stream loads to BigQuery", write);
    p.run().waitUntilFinish();

    List<TableRow> expectedRows = new ArrayList<>();
    for (long i = 0; i < TOTAL_N; i++) {
      expectedRows.add(generateRowFunc.apply(i));
    }

    // Perform checks
    checkRowCompleteness(tableSpec, inputSchema, expectedRows);
  }

  // Check that the expected rows reached the table.
  private static void checkRowCompleteness(
      String tableSpec, TableSchema schema, List<TableRow> expectedRows)
      throws IOException, InterruptedException {
    List<TableRow> actualTableRows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM [%s]", tableSpec), PROJECT, true, false, bigQueryLocation);

    Schema rowSchema = BigQueryUtils.fromTableSchema(schema);
    List<Row> actualBeamRows =
        actualTableRows.stream()
            .map(tableRow -> BigQueryUtils.toBeamRow(rowSchema, tableRow))
            .collect(Collectors.toList());
    List<Row> expectedBeamRows =
        expectedRows.stream()
            .map(tableRow -> BigQueryUtils.toBeamRow(rowSchema, tableRow))
            .collect(Collectors.toList());
    LOG.info(
        "Actual rows number: {}, expected: {}", actualBeamRows.size(), expectedBeamRows.size());

    assertThat(
        "Comparing expected rows with actual rows",
        actualBeamRows,
        containsInAnyOrder(expectedBeamRows.toArray()));
    assertEquals(
        "Checking there is no duplication", expectedBeamRows.size(), actualBeamRows.size());
  }

  @Test
  public void testLoadWithFixedShards() throws IOException, InterruptedException {
    runStreaming(5, false);
  }

  @Test
  public void testLoadWithAutoShardingAndCopyJobs() throws IOException, InterruptedException {
    runStreaming(0, true);
  }

  @Test
  public void testDynamicDestinationsWithFixedShards() throws IOException, InterruptedException {
    runStreamingToDynamicDestinations(6, false);
  }

  @Test
  public void testDynamicDestinationsWithAutoShardingAndCopyJobs()
      throws IOException, InterruptedException {
    runStreamingToDynamicDestinations(0, true);
  }

  private void runStreamingToDynamicDestinations(int numFileShards, boolean useCopyJobs)
      throws IOException, InterruptedException {
    TestPipelineOptions opts = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    opts.setTempLocation(opts.getTempRoot());
    Pipeline p = Pipeline.create(opts);
    // Only run the most relevant test cases on Dataflow. Testing this dimension on DirectRunner is
    // sufficient
    if (p.getOptions().getRunner().getName().contains("DataflowRunner")) {
      assumeTrue("Skipping in favor of more relevant test case", useInputSchema);
      // Need to manually enable streaming engine for legacy dataflow runner
      ExperimentalOptions.addExperiment(
          p.getOptions().as(ExperimentalOptions.class), GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    }

    List<String> allFields = Arrays.asList(FIELDS);
    List<String> subFields0 = new ArrayList<>(allFields.subList(0, 4));
    List<String> subFields1 = new ArrayList<>(allFields.subList(4, 8));
    List<String> subFields2 = new ArrayList<>(allFields.subList(8, 11));
    TableSchema table0Schema = makeTableSchemaFromTypes(subFields0);
    TableSchema table1Schema = makeTableSchemaFromTypes(subFields1);
    TableSchema table2Schema = makeTableSchemaFromTypes(subFields2);
    String table0Id = maybeCreateTable(table0Schema, "-0");
    String table1Id = maybeCreateTable(table1Schema, "-1");
    String table2Id = maybeCreateTable(table2Schema, "-2");
    GenerateRowFunc generateRowFunc0 = new GenerateRowFunc(subFields0);
    GenerateRowFunc generateRowFunc1 = new GenerateRowFunc(subFields1);
    GenerateRowFunc generateRowFunc2 = new GenerateRowFunc(subFields2);

    String tablePrefix = table0Id.substring(0, table0Id.length() - 2);

    // set up and build pipeline
    Instant start = new Instant(0);
    PCollection<Instant> instants =
        p.apply(
            "Generate Instants",
            PeriodicImpulse.create()
                .startAt(start)
                .stopAt(start.plus(Duration.standardSeconds(TOTAL_N - 1)))
                .withInterval(Duration.standardSeconds(1))
                .catchUpToNow(false));
    PCollection<Long> longs =
        instants.apply(
            "Create TableRows",
            MapElements.into(TypeDescriptors.longs()).via(instant -> instant.getMillis() / 1000));
    // build write transform
    Write<Long> write =
        BigQueryIO.<Long>write()
            .to(
                new TestDynamicDest(
                    tablePrefix, subFields0, subFields1, subFields2, useInputSchema))
            .withFormatFunction(
                id -> {
                  long dest = id % 3;
                  TableRow row;
                  if (dest == 0) {
                    row = generateRowFunc0.apply(id);
                  } else if (dest == 1) {
                    row = generateRowFunc1.apply(id);
                  } else {
                    row = generateRowFunc2.apply(id);
                  }
                  return row;
                })
            .withMethod(Write.Method.FILE_LOADS)
            .withTriggeringFrequency(Duration.standardSeconds(10));
    if (useCopyJobs) {
      write = write.withMaxBytesPerPartition(150);
    }
    if (useInputSchema) {
      // we're creating the table with the input schema
      write =
          write
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    } else {
      // table already exists with a schema, no need to create it
      write =
          write
              .withCreateDisposition(CreateDisposition.CREATE_NEVER)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    }
    write = numFileShards == 0 ? write.withAutoSharding() : write.withNumFileShards(numFileShards);

    longs.apply("Stream loads to dynamic destinations", write);
    p.run().waitUntilFinish();

    List<TableRow> expectedRows0 = new ArrayList<>();
    List<TableRow> expectedRows1 = new ArrayList<>();
    List<TableRow> expectedRows2 = new ArrayList<>();
    for (long i = 0; i < TOTAL_N; i++) {
      long dest = i % 3;
      if (dest == 0) {
        expectedRows0.add(generateRowFunc0.apply(i));
      } else if (dest == 1) {
        expectedRows1.add(generateRowFunc1.apply(i));
      } else {
        expectedRows2.add(generateRowFunc2.apply(i));
      }
    }
    // Perform checks
    checkRowCompleteness(table0Id, makeTableSchemaFromTypes(subFields0), expectedRows0);
    checkRowCompleteness(table1Id, makeTableSchemaFromTypes(subFields1), expectedRows1);
    checkRowCompleteness(table2Id, makeTableSchemaFromTypes(subFields2), expectedRows2);
  }

  static class TestDynamicDest extends DynamicDestinations<Long, Long> {
    String tablePrefix;
    List<String> table0Fields;
    List<String> table1Fields;
    List<String> table2Fields;
    boolean useInputSchema;

    public TestDynamicDest(
        String tablePrefix,
        List<String> table0Fields,
        List<String> table1Fields,
        List<String> table2Fields,
        boolean useInputSchema) {
      this.tablePrefix = tablePrefix;
      this.table0Fields = table0Fields;
      this.table1Fields = table1Fields;
      this.table2Fields = table2Fields;
      this.useInputSchema = useInputSchema;
    }

    @Override
    public Long getDestination(@Nullable ValueInSingleWindow<Long> element) {
      return element.getValue() % 3;
    }

    @Override
    public TableDestination getTable(Long destination) {
      return new TableDestination(tablePrefix + "-" + destination, null);
    }

    @Override
    public @Nullable TableSchema getSchema(Long destination) {
      if (!useInputSchema) {
        return null;
      }
      List<String> fields;
      if (destination == 0) {
        fields = table0Fields;
      } else if (destination == 1) {
        fields = table1Fields;
      } else {
        fields = table2Fields;
      }
      List<TableFieldSchema> tableFields =
          fields.stream()
              .map(name -> new TableFieldSchema().setName(name).setType(name).setMode("REQUIRED"))
              .collect(Collectors.toList());
      // we attach an ID to each row in addition to the existing schema fields
      tableFields.add(
          0, new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"));
      return new TableSchema().setFields(tableFields);
    }
  }
}
