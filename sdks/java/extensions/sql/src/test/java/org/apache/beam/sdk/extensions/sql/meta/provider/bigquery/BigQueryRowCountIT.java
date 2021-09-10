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

import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests form writing to BigQuery with Beam SQL. */
@RunWith(JUnit4.class)
public class BigQueryRowCountIT {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder().addNullableField("id", INT64).addNullableField("name", STRING).build();
  private static final String FAKE_JOB_NAME = "testPipelineOptionInjectionFakeJobName";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestPipeline readingPipeline = TestPipeline.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(SOURCE_SCHEMA);

  @Test
  public void testEmptyTable() {
    BigQueryTableProvider provider = new BigQueryTableProvider();
    Table table = getTable("testTable", bigQuery.tableSpec());
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);
    BeamTableStatistics size = sqlTable.getTableStatistics(TestPipeline.testingPipelineOptions());
    assertNotNull(size);
    assertEquals(0d, size.getRowCount(), 0.1);
  }

  @Test
  public void testNonEmptyTable() {
    BigQueryTableProvider provider = new BigQueryTableProvider();
    Table table = getTable("testTable", bigQuery.tableSpec());

    pipeline
        .apply(
            Create.of(
                    new TableRow().set("id", 1).set("name", "name1"),
                    new TableRow().set("id", 2).set("name", "name2"),
                    new TableRow().set("id", 3).set("name", "name3"))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(bigQuery.tableSpec())
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("id").setType("INTEGER"),
                                new TableFieldSchema().setName("name").setType("STRING"))))
                .withoutValidation());
    pipeline.run().waitUntilFinish();

    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);
    BeamTableStatistics size1 = sqlTable.getTableStatistics(TestPipeline.testingPipelineOptions());

    assertNotNull(size1);
    assertEquals(3d, size1.getRowCount(), 0.1);
  }

  /** This tests if the pipeline options are injected in the path of SQL Transform. */
  @Test
  public void testPipelineOptionInjection() {
    BigQueryTestTableProvider provider = new BigQueryTestTableProvider();
    Table table = getTable("testTable", bigQuery.tableSpec());
    provider.addTable("testTable", table);

    pipeline
        .apply(
            Create.of(
                    new TableRow().set("id", 1).set("name", "name1"),
                    new TableRow().set("id", 2).set("name", "name2"),
                    new TableRow().set("id", 3).set("name", "name3"))
                .withCoder(TableRowJsonCoder.of()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(bigQuery.tableSpec())
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("id").setType("INTEGER"),
                                new TableFieldSchema().setName("name").setType("STRING"))))
                .withoutValidation());
    pipeline.run().waitUntilFinish();

    // changing pipeline options
    readingPipeline.getOptions().setJobName(FAKE_JOB_NAME);

    // Reading from the table should update the statistics of bigQuery table
    readingPipeline.apply(
        SqlTransform.query(" select * from testTable ")
            .withDefaultTableProvider("bigquery", provider));

    readingPipeline.run().waitUntilFinish();

    BigQueryTestTable sqlTable = (BigQueryTestTable) provider.buildBeamSqlTable(table);
    assertEquals(FAKE_JOB_NAME, sqlTable.getJobName());
  }

  @Test
  public void testFakeTable() {
    BigQueryTableProvider provider = new BigQueryTableProvider();
    Table table = getTable("fakeTable", "project:dataset.table");

    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);
    BeamTableStatistics size = sqlTable.getTableStatistics(TestPipeline.testingPipelineOptions());
    assertTrue(size.isUnknown());
  }

  private static Table getTable(String name, String location) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(location)
        .schema(
            Stream.of(Schema.Field.nullable("id", INT64), Schema.Field.nullable("name", STRING))
                .collect(toSchema()))
        .type("bigquery")
        .build();
  }
}
