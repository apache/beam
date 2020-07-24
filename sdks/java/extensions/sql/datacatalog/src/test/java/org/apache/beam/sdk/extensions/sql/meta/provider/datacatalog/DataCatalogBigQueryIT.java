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

import com.google.api.services.bigquery.model.TableReference;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
    @Rule public transient TestPipeline writePipeline = TestPipeline.create();
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

    @SuppressWarnings("initialization.fields.uninitialized")
    @Parameterized.Parameter(0)
    public String dialectName;

    @SuppressWarnings("initialization.fields.uninitialized")
    @Parameterized.Parameter(1)
    public Class<? extends QueryPlanner> queryPlanner;

    @SuppressWarnings("nullness")
    @Test
    public void testRead() throws Exception {
      TableReference bqTable = bigQuery.tableReference();

      // Streaming inserts do not work with DIRECT_READ mode, there is a several hour lag.
      PCollection<Row> data =
          writePipeline.apply(Create.of(row(1, "name1"), row(2, "name2"), row(3, "name3")));
      data.apply(
          BigQueryIO.<Row>write()
              .withSchema(BigQueryUtils.toTableSchema(ID_NAME_SCHEMA))
              .withFormatFunction(BigQueryUtils.toTableRow())
              .withMethod(Method.FILE_LOADS)
              .to(bqTable));
      writePipeline.run().waitUntilFinish(Duration.standardMinutes(2));

      String tableId =
          String.format(
              "bigquery.`table`.`%s`.`%s`.`%s`",
              bqTable.getProjectId(), bqTable.getDatasetId(), bqTable.getTableId());

      readPipeline
          .getOptions()
          .as(BeamSqlPipelineOptions.class)
          .setPlannerName(queryPlanner.getCanonicalName());

      try (DataCatalogTableProvider tableProvider =
          DataCatalogTableProvider.create(
              readPipeline.getOptions().as(DataCatalogPipelineOptions.class))) {
        PCollection<Row> result =
            readPipeline.apply(
                "query",
                SqlTransform.query("SELECT id, name FROM " + tableId)
                    .withDefaultTableProvider("datacatalog", tableProvider));

        PAssert.that(result).containsInAnyOrder(row(1, "name1"), row(2, "name2"), row(3, "name3"));
        readPipeline.run().waitUntilFinish(Duration.standardMinutes(2));
      }
    }

    private static Row row(long id, String name) {
      return Row.withSchema(ID_NAME_SCHEMA).addValues(id, name).build();
    }
  }
}
