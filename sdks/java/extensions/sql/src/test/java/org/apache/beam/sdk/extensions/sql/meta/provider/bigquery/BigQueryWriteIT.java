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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableRow;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests form writing to BigQuery with Beam SQL. */
@RunWith(JUnit4.class)
public class BigQueryWriteIT implements Serializable {

  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT64)
          .addNullableField("name", Schema.FieldType.STRING)
          .build();

  private static final Schema RESULT_SCHEMA =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.STRING)
          .addNullableField("name", Schema.FieldType.STRING)
          .build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(SOURCE_SCHEMA);

  @Test
  public void testInsertValues() throws Exception {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE TABLE ORDERS( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR ) \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement = "INSERT INTO ORDERS VALUES (1, 'foo')";

    PCollectionTuple.empty(pipeline).apply(sqlEnv.parseQuery(insertStatement));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    bigQuery.assertContainsInAnyOrder(toTableRow(row(RESULT_SCHEMA, "1", "foo")));
  }

  @Test
  public void testInsertSelect() throws Exception {
    BeamSqlEnv sqlEnv =
        BeamSqlEnv.inMemory(
            readOnlyTableProvider(
                pipeline,
                "ORDERS_IN_MEMORY",
                row(SOURCE_SCHEMA, 1L, "foo"),
                row(SOURCE_SCHEMA, 2L, "bar"),
                row(SOURCE_SCHEMA, 3L, "baz")),
            new BigQueryTableProvider());

    String createTableStatement =
        "CREATE TABLE ORDERS_BQ( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR ) \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement = "INSERT INTO ORDERS_BQ \n" + "SELECT id, name FROM ORDERS_IN_MEMORY";
    PCollectionTuple.empty(pipeline).apply(sqlEnv.parseQuery(insertStatement));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    bigQuery.assertContainsInAnyOrder(
        toTableRow(row(RESULT_SCHEMA, "1", "foo")),
        toTableRow(row(RESULT_SCHEMA, "2", "bar")),
        toTableRow(row(RESULT_SCHEMA, "3", "baz")));
  }

  private TableProvider readOnlyTableProvider(Pipeline pipeline, String tableName, Row... rows) {

    return new ReadOnlyTableProvider(
        "PCOLLECTION",
        ImmutableMap.of(tableName, new BeamPCollectionTable(createPCollection(pipeline, rows))));
  }

  private PCollection<Row> createPCollection(Pipeline pipeline, Row... rows) {
    return pipeline.apply(Create.of(Arrays.asList(rows)).withCoder(SOURCE_SCHEMA.getRowCoder()));
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
