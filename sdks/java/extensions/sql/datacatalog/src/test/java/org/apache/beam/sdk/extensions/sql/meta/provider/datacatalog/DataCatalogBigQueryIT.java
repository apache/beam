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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration tests for DataCatalog+BigQuery. */
@RunWith(Enclosed.class)
public class DataCatalogBigQueryIT {

  @RunWith(Parameterized.class)
  public static class DialectSensitiveTests {
    private static final Schema ID_NAME_SCHEMA =
        Schema.builder().addNullableField("id", INT64).addNullableField("name", STRING).build();
    @Rule public transient TestPipeline writeToBQPipeline = TestPipeline.create();
    @Rule public transient TestPipeline readPipeline = TestPipeline.create();
    @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(ID_NAME_SCHEMA);

    /** Parameterized by which SQL dialect, since the syntax here is the same. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> dialects() {
      return Arrays.asList(
          new Object[][] {
            {"ZetaSQL", ZetaSQLQueryPlanner.class},
            {"CalciteSQL", CalciteQueryPlanner.class}
          });
    }

    @Parameterized.Parameter(0)
    public String dialectName;

    @Parameterized.Parameter(1)
    public Class<? extends QueryPlanner> queryPlanner;

    @Test
    public void testReadWrite() throws Exception {
      writeToBQPipeline.apply(
          createBqTable(
              new TableRow().set("id", 1).set("name", "name1"),
              new TableRow().set("id", 2).set("name", "name2"),
              new TableRow().set("id", 3).set("name", "name3")));
      writeToBQPipeline.run().waitUntilFinish(Duration.standardMinutes(2));

      TableReference bqTable = bigQuery.tableReference();
      String tableId =
          String.format(
              "bigquery.`table`.`%s`.`%s`.`%s`",
              bqTable.getProjectId(), bqTable.getDatasetId(), bqTable.getTableId());

      readPipeline
          .getOptions()
          .as(BeamSqlPipelineOptions.class)
          .setPlannerName(queryPlanner.getCanonicalName());

      PCollection<Row> result =
          readPipeline.apply(
              "query",
              SqlTransform.query("SELECT id, name FROM " + tableId)
                  .withDefaultTableProvider(
                      "datacatalog",
                      DataCatalogTableProvider.create(
                          readPipeline.getOptions().as(DataCatalogPipelineOptions.class))));

      PAssert.that(result).containsInAnyOrder(row(1, "name1"), row(2, "name2"), row(3, "name3"));
      readPipeline.run().waitUntilFinish(Duration.standardMinutes(2));
    }

    private static Row row(long id, String name) {
      return Row.withSchema(ID_NAME_SCHEMA).addValues(id, name).build();
    }

    public CreateBqTable createBqTable(TableRow... rows) {
      return new CreateBqTable(Arrays.asList(rows));
    }

    private class CreateBqTable extends PTransform<PInput, WriteResult> {

      private final List<TableRow> rows;

      private CreateBqTable(List<TableRow> rows) {
        this.rows = rows;
      }

      @Override
      public WriteResult expand(PInput input) {
        return input
            .getPipeline()
            .begin()
            .apply(Create.<TableRow>of(rows).withCoder(TableRowJsonCoder.of()))
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
      }
    }
  }
}
