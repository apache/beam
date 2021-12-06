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
package org.apache.beam.sdk.tpcds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes jobs using PCollection and SqlTransform, it uses SqlTransform.query to run
 * queries.
 *
 * <p>TODO: Add tests.
 */
public class SqlTransformRunner {
  private static final String SUMMARY_START = "\n" + "TPC-DS Query Execution Summary:";
  private static final List<String> SUMMARY_HEADERS_LIST =
      Arrays.asList(
          "Query Name",
          "Job Name",
          "Data Size",
          "Dialect",
          "Status",
          "Start Time",
          "End Time",
          "Elapsed Time(sec)");

  private static final Logger LOG = LoggerFactory.getLogger(SqlTransformRunner.class);

  /** This class is used to extract all SQL query identifiers. */
  static class SqlIdentifierVisitor extends SqlBasicVisitor<Void> {
    private final Set<String> identifiers = new HashSet<>();

    public Set<String> getIdentifiers() {
      return identifiers;
    }

    @Override
    public Void visit(SqlIdentifier id) {
      identifiers.addAll(id.names);
      return null;
    }
  }

  /**
   * Get all tables (in the form of TextTable) needed for a specific query execution.
   *
   * @param pipeline The pipeline that will be run to execute the query
   * @param csvFormat The csvFormat to construct readConverter (CsvToRow) and writeConverter
   *     (RowToCsv)
   * @param queryName The name of the query which will be executed (for example: query3, query55,
   *     query96)
   * @return A PCollectionTuple which is constructed by all tables needed for running query.
   * @throws Exception
   */
  private static PCollectionTuple getTables(
      Pipeline pipeline, CSVFormat csvFormat, String queryName) throws Exception {
    Map<String, Schema> schemaMap = TpcdsSchemas.getTpcdsSchemas();
    TpcdsOptions tpcdsOptions = pipeline.getOptions().as(TpcdsOptions.class);
    String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
    Set<String> identifiers = QueryReader.getQueryIdentifiers(QueryReader.readQuery(queryName));

    PCollectionTuple tables = PCollectionTuple.empty(pipeline);
    for (Map.Entry<String, Schema> tableSchema : schemaMap.entrySet()) {
      String tableName = tableSchema.getKey();

      // Only when query identifiers contain tableName, the table is relevant to this query and will
      // be added. This can avoid reading unnecessary data files.
      if (identifiers.contains(tableName.toUpperCase())) {
        Set<String> tableColumns = getTableColumns(identifiers, tableSchema);

        switch (tpcdsOptions.getSourceType()) {
          case CSV:
            {
              PCollection<Row> table =
                  getTableCSV(pipeline, csvFormat, tpcdsOptions, dataSize, tableSchema, tableName);
              tables = tables.and(new TupleTag<>(tableName), table);
              break;
            }
          case PARQUET:
            {
              PCollection<GenericRecord> table =
                  getTableParquet(pipeline, tpcdsOptions, dataSize, tableName, tableColumns);
              tables = tables.and(new TupleTag<>(tableName), table);
              break;
            }
          default:
            throw new IllegalStateException(
                "Unexpected source type: " + tpcdsOptions.getSourceType());
        }
      }
    }
    return tables;
  }

  private static Set<String> getTableColumns(
      Set<String> identifiers, Map.Entry<String, Schema> tableSchema) {
    Set<String> tableColumns = new HashSet<>();
    List<Schema.Field> fields = tableSchema.getValue().getFields();
    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      if (identifiers.contains(fieldName.toUpperCase())) {
        tableColumns.add(fieldName);
      }
    }
    return tableColumns;
  }

  private static PCollection<GenericRecord> getTableParquet(
      Pipeline pipeline,
      TpcdsOptions tpcdsOptions,
      String dataSize,
      String tableName,
      Set<String> tableColumns)
      throws IOException {
    org.apache.avro.Schema schema = getAvroSchema(tableName);
    org.apache.avro.Schema schemaProjected = getProjectedSchema(tableColumns, schema);

    String filepattern =
        tpcdsOptions.getDataDirectory() + "/" + dataSize + "/" + tableName + "/*.parquet";

    return pipeline.apply(
        "Read " + tableName + " (parquet)",
        ParquetIO.read(schema)
            .from(filepattern)
            .withSplit()
            .withProjection(schemaProjected, schemaProjected)
            .withBeamSchemas(true));
  }

  private static PCollection<Row> getTableCSV(
      Pipeline pipeline,
      CSVFormat csvFormat,
      TpcdsOptions tpcdsOptions,
      String dataSize,
      Map.Entry<String, Schema> tableSchema,
      String tableName) {
    // This is location path where the data are stored
    String filePattern =
        tpcdsOptions.getDataDirectory() + "/" + dataSize + "/" + tableName + ".dat";

    return new TextTable(
            tableSchema.getValue(),
            filePattern,
            new CsvToRow(tableSchema.getValue(), csvFormat),
            new RowToCsv(csvFormat))
        .buildIOReader(pipeline.begin())
        .setCoder(SchemaCoder.of(tableSchema.getValue()))
        .setName(tableSchema.getKey());
  }

  private static org.apache.avro.Schema getAvroSchema(String tableName) throws IOException {
    String path = "schemas_avro/" + tableName + ".json";
    return new org.apache.avro.Schema.Parser()
        .parse(Resources.toString(Resources.getResource(path), Charsets.UTF_8));
  }

  static org.apache.avro.Schema getProjectedSchema(
      Set<String> projectedFieldNames, org.apache.avro.Schema schema) {
    List<org.apache.avro.Schema.Field> projectedFields = new ArrayList<>();
    for (org.apache.avro.Schema.Field f : schema.getFields()) {
      if (projectedFieldNames.contains(f.name())) {
        projectedFields.add(
            new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
      }
    }
    org.apache.avro.Schema schemaProjected =
        org.apache.avro.Schema.createRecord(schema.getName() + "_projected", "", "", false);
    schemaProjected.setFields(projectedFields);
    return schemaProjected;
  }

  /**
   * Print the summary table after all jobs are finished.
   *
   * @param completion A collection of all TpcdsRunResult that are from finished jobs.
   * @param numOfResults The number of results in the collection.
   * @throws Exception
   */
  private static void printExecutionSummary(
      CompletionService<TpcdsRunResult> completion, int numOfResults) throws Exception {
    List<List<String>> summaryRowsList = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      TpcdsRunResult tpcdsRunResult = completion.take().get();
      List<String> list = new ArrayList<>();
      list.add(tpcdsRunResult.getQueryName());
      list.add(tpcdsRunResult.getJobName());
      list.add(tpcdsRunResult.getDataSize());
      list.add(tpcdsRunResult.getDialect());
      // If the job is not successful, leave the run time related field blank
      list.add(tpcdsRunResult.getIsSuccessful() ? "Successful" : "Failed");
      list.add(tpcdsRunResult.getIsSuccessful() ? tpcdsRunResult.getStartDate().toString() : "");
      list.add(tpcdsRunResult.getIsSuccessful() ? tpcdsRunResult.getEndDate().toString() : "");
      list.add(
          tpcdsRunResult.getIsSuccessful() ? Double.toString(tpcdsRunResult.getElapsedTime()) : "");
      summaryRowsList.add(list);
    }

    System.out.println(SUMMARY_START);
    System.out.println(SummaryGenerator.generateTable(SUMMARY_HEADERS_LIST, summaryRowsList));
  }

  /**
   * This is the default method in BeamTpcds.main method. Run job using SqlTranform.query() method.
   *
   * @param args Command line arguments
   * @throws Exception
   */
  public static void runUsingSqlTransform(String[] args) throws Exception {
    TpcdsOptions tpcdsOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);

    String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
    String[] queryNames = TpcdsParametersReader.getAndCheckQueryNames(tpcdsOptions);
    int nThreads = TpcdsParametersReader.getAndCheckTpcParallel(tpcdsOptions);

    // Using ExecutorService and CompletionService to fulfill multi-threading functionality
    ExecutorService executor = Executors.newFixedThreadPool(nThreads);
    CompletionService<TpcdsRunResult> completion = new ExecutorCompletionService<>(executor);

    // Make an array of pipelines, each pipeline is responsible for running a corresponding query.
    Pipeline[] pipelines = new Pipeline[queryNames.length];
    CSVFormat csvFormat = CSVFormat.MYSQL.withDelimiter('|').withNullString("");

    // Execute all queries, transform each result into a PCollection<String>, write them into
    // the txt file and store in a GCP directory.
    for (int i = 0; i < queryNames.length; i++) {
      // For each query, get a copy of pipelineOptions from command line arguments.
      TpcdsOptions tpcdsOptionsCopy =
          PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);

      // Set a unique job name using the time stamp so that multiple different pipelines can run
      // together.
      tpcdsOptionsCopy.setJobName(queryNames[i] + "result" + System.currentTimeMillis());

      pipelines[i] = Pipeline.create(tpcdsOptionsCopy);
      String queryString = QueryReader.readQuery(queryNames[i]);
      PCollectionTuple tables = getTables(pipelines[i], csvFormat, queryNames[i]);

      try {
        tables
            .apply(SqlTransform.query(queryString))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Row::toString))
            .apply(
                TextIO.write()
                    .to(
                        tpcdsOptions.getResultsDirectory()
                            + "/"
                            + dataSize
                            + "/"
                            + pipelines[i].getOptions().getJobName())
                    .withSuffix(".txt")
                    .withNumShards(1));
      } catch (Exception e) {
        LOG.error("{} failed to execute", queryNames[i]);
        e.printStackTrace();
      }

      completion.submit(new TpcdsRun(pipelines[i]));
    }

    executor.shutdown();

    printExecutionSummary(completion, queryNames.length);
  }
}
