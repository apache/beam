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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.BeamSqlTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests form writing to BigQuery with Beam SQL.
 */
@RunWith(JUnit4.class)
public class BigQueryWriteIT implements Serializable {

  private static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("YYYY_MM_dd_HH_mm_ss_SSS");

  private static final Schema SOURCE_SCHEMA =
      Schema.builder().addInt64Field("id").addStringField("name").build();

  private static final Schema RESULT_SCHEMA =
      Schema.builder().addStringField("id").addStringField("name").build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestPipeline resultsPipeline = TestPipeline.create();

  @Test
  public void testInsertValues() throws Exception {
    String bigQueryLocation = createBQTableName("testInsertValues");
    BeamSqlEnv sqlEnv = newSqlEnv(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE TABLE ORDERS( \n"
        + "   id BIGINT, \n"
        + "   name VARCHAR ) \n"
        + "TYPE 'bigquery' \n"
        + "LOCATION '" + bigQueryLocation + "'";
    executeCreateTable(sqlEnv, createTableStatement);

    executeInsert(pipeline, sqlEnv, "INSERT INTO ORDERS VALUES (1, 'foo')");

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));

    PAssert
        .that(allRowsFromBQ(resultsPipeline, bigQueryLocation))
        .containsInAnyOrder(row(RESULT_SCHEMA, "1", "foo"));
    resultsPipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testInsertSelect() throws Exception {
    String bigQueryLocation = createBQTableName("testInsertSelect");

    BeamSqlEnv sqlEnv = newSqlEnv(
        inMemoryTableProvider(
            pipeline,
            "ORDERS_IN_MEMORY",
            row(SOURCE_SCHEMA, 1L, "foo"),
            row(SOURCE_SCHEMA, 2L, "bar"),
            row(SOURCE_SCHEMA, 3L, "baz")),
        new BigQueryTableProvider()
    );

    String createTableStatement =
        "CREATE TABLE ORDERS_BQ( \n"
        + "   id BIGINT, \n"
        + "   name VARCHAR ) \n"
        + "TYPE 'bigquery' \n"
        + "LOCATION '" + bigQueryLocation + "'";
    executeCreateTable(sqlEnv, createTableStatement);

    String insertStatement =
        "INSERT INTO ORDERS_BQ \n"
        + "SELECT id, name FROM ORDERS_IN_MEMORY";
    executeInsert(pipeline, sqlEnv, insertStatement);

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));

    PAssert
        .that(allRowsFromBQ(resultsPipeline, bigQueryLocation))
        .containsInAnyOrder(
            row(RESULT_SCHEMA, "1", "foo"),
            row(RESULT_SCHEMA, "2", "bar"),
            row(RESULT_SCHEMA, "3", "baz"));
    resultsPipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  private TableProvider inMemoryTableProvider(
      Pipeline pipeline,
      String tableName,
      Row ... rows) {

    return
        new BeamSqlTableProvider(
            "PCOLLECTION",
            ImmutableMap.of(
                tableName,
                new BeamPCollectionTable(createPCollection(pipeline, rows))));
  }

  private PCollection<Row> createPCollection(Pipeline pipeline, Row ... rows) {
    return pipeline.apply(Create.of(Arrays.asList(rows)).withCoder(SOURCE_SCHEMA.getRowCoder()));
  }

  private String createBQTableName(String testName) throws Exception {
    TestBigQueryOptions pipelineOptions =
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class);
    String project = pipelineOptions.getProject();
    String dataSet = pipelineOptions.getIntegrationTestDataset();
    String table = createRandomizedName(testName);
    return String.format("%s:%s.%s", project, dataSet, table);
  }

  private PCollection<Row> allRowsFromBQ(Pipeline pipeline, String bigQueryLocation) {
    return pipeline
        .apply(
            BigQueryIO
            .readTableRows()
            .from(bigQueryLocation))
        .apply(toResultRow());
  }

  private MapElements<TableRow, Row> toResultRow() {
    return MapElements.via(new SimpleFunction<TableRow, Row>() {
      @Override
      public Row apply(TableRow bqRow) {
        List<Object> bqRowValues =
            RESULT_SCHEMA.getFields()
                .stream()
                .map(f -> bqRow.get(f.getName()))
                .collect(Collectors.toList());

        return Row.withSchema(RESULT_SCHEMA).addValues(bqRowValues).build();
      }
    });
  }

  private BeamSqlEnv newSqlEnv(TableProvider ... tableProviders) {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      metaStore.registerProvider(tableProvider);
    }
    return new BeamSqlEnv(metaStore);
  }

  private void executeCreateTable(BeamSqlEnv sqlEnv, String statement) throws SqlParseException {
    SqlNode sqlNode = sqlEnv.getPlanner().parse(statement);
    ((SqlExecutableStatement) sqlNode).execute(sqlEnv.getContext());
  }

  private PCollection<Row> executeInsert(
      Pipeline pipeline,
      BeamSqlEnv sqlEnv,
      String statement) throws Exception {

    return sqlEnv.getPlanner().compileBeamPipeline(statement, pipeline);
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  private String createRandomizedName(String testName) throws Exception {
    StringBuilder tableName = new StringBuilder()
        .append(BigQueryWriteIT.class.getSimpleName())
        .append("_")
        .append(testName)
        .append("_");
    DATETIME_FORMAT.printTo(tableName, Instant.now());
    return tableName.toString() + "_"
           + String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong()));
  }
}
