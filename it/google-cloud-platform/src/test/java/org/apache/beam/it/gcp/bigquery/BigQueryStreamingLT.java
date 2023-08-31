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
package org.apache.beam.it.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableReference;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSpec;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryStreamingLT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStreamingLT.class);

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryStreamingLT");
  private static final String PROJECT = "google.com:clouddfe";
  //            TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

  private static final String BIG_QUERY_DATASET_ID = "ahmedabualsaud_test";
  //            "storage_api_sink_load_test_" + System.nanoTime();

  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    //        BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  //    @AfterClass
  //    public static void cleanup() {
  //        BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  //    }

  public interface BigQueryStreamingLoadTestOptions extends TestPipelineOptions, BigQueryOptions {
    @Default.String("")
    @Description("The destination table to write to.")
    String getDestinationTable();

    void setDestinationTable(String destinationTable);

    @Default.String("")
    @Description(
        "The expected table to check against for correctness. If unset, the test will run a batch job with FILE_LOADS and use the resulting table as a source of truth.")
    String getExpectedTable();

    void setExpectedTable(String expectedTable);

    @Default.Integer(10_000)
    @Description(
        "Rate of generated elements sent to the sink. Will run with a minimum of 1k rows per second.")
    Integer getRowsPerSecond();

    void setRowsPerSecond(Integer rowsPerSecond);

    @Default.Integer(15)
    @Description("Rows will be generated for this many minutes.")
    Integer getMinutes();

    void setMinutes(Integer minutes);

    @Default.Integer(5)
    @Description("Data shape: The number of fields per row.")
    Integer getNumFields();

    void setNumFields(Integer numFields);

    @Default.Integer(100)
    @Description("Data shape: The byte-size for each field.")
    Integer getByteSizePerField();

    void setByteSizePerField(Integer byteSizePerField);
  }

  public BigQueryStreamingLoadTestOptions getOptions(String size, boolean crash) {
    BigQueryStreamingLoadTestOptions options =
        TestPipeline.testingPipelineOptions().as(BigQueryStreamingLoadTestOptions.class);
    switch (size) {
      case "large":
        // 1.8B rows, >180 TB
        // 100K rows/s, >10GB/s
        options.setMinutes(300);
        options.setByteSizePerField(1000);
        options.setNumFields(100);
        options.setRowsPerSecond(100_000);
        if (crash) {
          // crash every 20 min
          options.setCrashStorageApiWriteEverySeconds(1200L);
        }
        break;
      case "medium":
        // 36M rows, >36 GB
        // 10K rows/s, >10MB/s
        options.setMinutes(60);
        options.setByteSizePerField(100);
        options.setNumFields(10);
        options.setRowsPerSecond(10_000);
        if (crash) {
          // crash every 5 min
          options.setCrashStorageApiWriteEverySeconds(300L);
        }
        break;
      default: // small
        // 300K rows, >3 MB
        // 1K rows/s, >10KB/s
        options.setMinutes(5);
        options.setByteSizePerField(5);
        options.setNumFields(2);
        options.setRowsPerSecond(1000);
    }
    return options;
  }

  @Test
  public void testExactlyOnceStreaming() throws IOException, InterruptedException {
    BigQueryStreamingLoadTestOptions options = getOptions("medium", true);
    options.setUseStorageWriteApi(true);
    runTest(options);
  }

  @Test
  public void testAtLeastOnceStreaming() throws IOException, InterruptedException {
    BigQueryStreamingLoadTestOptions options = getOptions("medium", true);
    options.setUseStorageWriteApiAtLeastOnce(true);
    runTest(options);
  }

  public void runTest(BigQueryStreamingLoadTestOptions options)
      throws IOException, InterruptedException {
    long millis = Duration.standardMinutes(options.getMinutes()).getMillis();
    int rowsPerSecond = Math.max(options.getRowsPerSecond(), 1000);

    // The PeriodicImpulse source will generate an element every this many millis:
    int fireInterval = 1;
    // Each element from PeriodicImpulse will fan out to this many elements
    // (applicable when a high row-per-second rate is set)
    long multiplier = rowsPerSecond / 1000;
    long totalRows = multiplier * millis / fireInterval;

    String expectedTable = options.getExpectedTable();
    GenerateTableRow genRow =
        new GenerateTableRow(options.getNumFields(), options.getByteSizePerField());
    TableSchema schema = generateTableSchema(options.getNumFields());

    if (Strings.isNullOrEmpty(expectedTable)) {
      expectedTable =
          String.format(
              "%s.%s.fileloads-%s-records",
              PROJECT, BIG_QUERY_DATASET_ID, withScaleSymbol(totalRows));
      LOG.info(
          "No expected table was set. Will run a batch job to load {} rows to {}."
              + " This will be used as the source of truth.",
          totalRows,
          expectedTable);

      Pipeline q = Pipeline.create(TestPipeline.testingPipelineOptions());
      q.apply(GenerateSequence.from(0).to(totalRows))
          .apply(
              BigQueryIO.<Long>write()
                  .to(expectedTable)
                  .withFormatFunction(genRow::apply)
                  .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                  .withSchema(schema));
      q.run();
    }

    String destTable = options.getDestinationTable();
    if (Strings.isNullOrEmpty(destTable)) {
      destTable =
          String.format(
              "%s.%s.storageapi-load-%sqps-%smin-%stotal",
              PROJECT,
              BIG_QUERY_DATASET_ID,
              withScaleSymbol(rowsPerSecond),
              options.getMinutes(),
              withScaleSymbol(totalRows));
    }
    LOG.info(
        "Preparing a source generating at a rate of {} rows per second for a period of {} minutes."
            + " This results in a total of {} rows written to {}.",
        rowsPerSecond,
        options.getMinutes(),
        totalRows,
        destTable);
    if (options.getRunner().equals(DataflowRunner.class)
        || options.getRunner().equals(TestDataflowRunner.class)) {
      options.as(DataflowPipelineOptions.class).setStreaming(true);
      options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(true);
    }
    Pipeline p = Pipeline.create(options);

    Instant start = Instant.now();
    PCollection<Long> source =
        p.apply(
                PeriodicImpulse.create()
                    .startAt(start)
                    .stopAt(start.plus(Duration.millis(millis - 1)))
                    .withInterval(Duration.millis(fireInterval)))
            .apply(
                MapElements.into(TypeDescriptors.longs())
                    .via(instant -> instant.getMillis() % totalRows));
    if (multiplier > 1) {
      source =
          source
              .apply(ParDo.of(new MultiplierDoFn(multiplier)))
              .apply("Reshuffle fanout", Reshuffle.viaRandomKey());
    }
    BigQueryIO.Write.Method method =
        options.getUseStorageWriteApiAtLeastOnce()
            ? BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE
            : BigQueryIO.Write.Method.STORAGE_WRITE_API;
    source.apply(
        BigQueryIO.<Long>write()
            .to(destTable)
            .withFormatFunction(id -> genRow.apply(id))
            .withMethod(method)
            .withTriggeringFrequency(Duration.standardSeconds(1))
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchema(schema));
    p.run().waitUntilFinish();

    LOG.info(
        "Write pipeline finished writing to {}. Will now perform accuracy checks against the rows in {}.",
        destTable,
        expectedTable);
    // Filter our structs and arrays, so we can use `EXCEPT DISTINCT` when we compare with query
    String columnNames =
        schema.getFields().stream()
            .map(TableFieldSchema::getName)
            .filter(fieldName -> fieldName.startsWith(FIELD_PREFIX))
            .collect(Collectors.joining(", "));
    checkCorrectness(columnNames, destTable, expectedTable);
    // check non-duplication for STORAGE_WRITE_API
    if (method == BigQueryIO.Write.Method.STORAGE_WRITE_API) {
      checkNonDuplication(destTable, expectedTable, totalRows);
    }
  }

  public void checkCorrectness(String columnNames, String destTable, String expectedTable)
      throws IOException, InterruptedException {
    // Need table spec to be in the format `myproject.mydataset.mytable` to include in BQ queries.
    destTable = toTableSpec(toTableReference(destTable));
    expectedTable = toTableSpec(toTableReference(expectedTable));

    String checkCorrectnessQuery =
        String.format(
            "WITH \n"
                + "storage_api_table AS (SELECT %s FROM `%s`), \n"
                + "expected_table AS (SELECT %s FROM `%s`), \n"
                + "rows_mismatched AS (SELECT * FROM expected_table EXCEPT DISTINCT SELECT * FROM storage_api_table) \n"
                + "SELECT COUNT(*) FROM rows_mismatched",
            columnNames, destTable, columnNames, expectedTable);

    LOG.info("Executing query to check correctness:\n{}", checkCorrectnessQuery);

    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(checkCorrectnessQuery, "google.com:clouddfe", true, true));
    long result = Long.parseLong((String) queryResponse.get("f0_"));

    LOG.info("Number of mismatched rows: {}", result);
    assertEquals(
        String.format("Saw %s rows that are missing from %s.", result, destTable), 0, result);
  }

  public void checkNonDuplication(String destTable, String expectedTable, long totalRows)
      throws IOException, InterruptedException {
    String checkDuplicationQuery =
        String.format(
            "SELECT \n"
                + "(SELECT COUNT(*) FROM  `%s`) AS actualCount,\n"
                + "(SELECT COUNT(*) FROM  `%s`) AS expectedCount",
            destTable, expectedTable);

    LOG.info("Executing query to check non-duplication:\n{}", checkDuplicationQuery);

    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(checkDuplicationQuery, "google.com:clouddfe", true, true));
    long actualCount = Long.parseLong((String) queryResponse.get("actualCount"));
    long expectedCount = Long.parseLong((String) queryResponse.get("expectedCount"));
    assertEquals(
        "Comparing actual table count and expected table count.", expectedCount, actualCount);
    assertEquals(
        "Comparing actual table count and calculated expected count.", totalRows, actualCount);
  }

  // From a value, get the appropriate shortened name that includes the scale
  // For example, from 12,345,678 return 12M
  public String withScaleSymbol(long value) {
    List<String> scales = Arrays.asList("", "K", "M", "B", "T", "Q");
    int scaleIndex = 0;
    while (value / 1000 > 0) {
      scaleIndex++;
      value /= 1000;
    }

    return String.format("%s%s", value, scales.get(scaleIndex));
  }

  public static class MultiplierDoFn extends DoFn<Long, Long> {
    private long multiplier;

    MultiplierDoFn(long multiplier) {
      this.multiplier = multiplier;
    }

    @ProcessElement
    public void processElement(@Element Long element, OutputReceiver<Long> outputReceiver) {
      for (int i = 0; i < multiplier; i++) {
        outputReceiver.output(element);
      }
    }
  }

  static final String FIELD_PREFIX = "byte_field_";
  static final String RECORD_FIELD_PREFIX = "record_" + FIELD_PREFIX;
  static final String NESTED_FIELD_PREFIX = "nested_" + FIELD_PREFIX;
  static final String REPEATED_FIELD_PREFIX = "repeated_" + FIELD_PREFIX;

  public static TableSchema generateTableSchema(int numFields) {
    List<TableFieldSchema> fields = new ArrayList<>(numFields);
    fields.add(new TableFieldSchema().setType("INTEGER").setName("id"));
    int j = 1;
    for (int i = 1; i <= numFields; i++) {
      TableFieldSchema fieldSchema = new TableFieldSchema();
      // Every 4th field will be a struct, every 5th field will be an array
      if (j == 4) {
        fieldSchema
            .setType("RECORD")
            .setName(RECORD_FIELD_PREFIX + i)
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setType("BYTES").setName(NESTED_FIELD_PREFIX + 1),
                    new TableFieldSchema().setType("BYTES").setName(NESTED_FIELD_PREFIX + 2)));
      } else if (j == 5) {
        fieldSchema.setType("BYTES").setMode("REPEATED").setName(REPEATED_FIELD_PREFIX + i);
        j = 0;
      } else {
        fieldSchema.setType("BYTES").setName(FIELD_PREFIX + i);
      }
      j++;
      fields.add(fieldSchema);
    }
    return new TableSchema().setFields(fields);
  }

  static class GenerateTableRow implements SerializableFunction<Long, TableRow> {
    private final int numFields;
    private final int sizePerField;

    public GenerateTableRow(int numFields, int sizePerField) {
      assert numFields >= 0;
      this.numFields = numFields;
      this.sizePerField = sizePerField;
    }

    @Override
    public TableRow apply(Long rowId) {
      TableRow row = new TableRow();
      row.set("id", rowId);
      byte[] payload = getPayload(sizePerField, rowId).array();
      int j = 1;
      for (int i = 1; i <= numFields; i++) {
        // TODO: we can also make the struct and array sizes variable
        if (j == 4) {
          row.set(
              RECORD_FIELD_PREFIX + i,
              new TableRow()
                  .set(NESTED_FIELD_PREFIX + 1, Arrays.copyOfRange(payload, 0, sizePerField / 2))
                  .set(
                      NESTED_FIELD_PREFIX + 2,
                      Arrays.copyOfRange(payload, sizePerField / 2, sizePerField)));
        } else if (j == 5) {
          row.set(
              REPEATED_FIELD_PREFIX + i,
              Arrays.asList(
                  Arrays.copyOfRange(payload, 0, sizePerField / 3),
                  Arrays.copyOfRange(payload, sizePerField / 3, sizePerField * 2 / 3),
                  Arrays.copyOfRange(payload, sizePerField * 2 / 3, sizePerField)));
          j = 0;
        } else {
          row.set(FIELD_PREFIX + i, payload);
        }
        j++;
      }
      return row;
    }

    private @Nullable ByteBuffer getPayload(int payloadSize, long rowId) {
      if (payloadSize <= 0) {
        return null;
      }
      byte[] payload = new byte[payloadSize];
      Random localRandom = ThreadLocal.withInitial(() -> new Random(rowId)).get();
      localRandom.setSeed(rowId);
      localRandom.nextBytes(payload);

      return ByteBuffer.wrap(payload);
    }
  }
}
