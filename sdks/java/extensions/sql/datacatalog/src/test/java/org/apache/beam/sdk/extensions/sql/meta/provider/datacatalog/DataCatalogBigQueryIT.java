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
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for DataCatalog+BigQuery. */
@RunWith(JUnit4.class)
public class DataCatalogBigQueryIT {

  private static final Schema ID_NAME_SCHEMA =
      Schema.builder().addNullableField("id", INT64).addNullableField("name", STRING).build();

  @Rule public transient TestPipeline writeToBQPipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(ID_NAME_SCHEMA);

  @Test
  public void testReadWrite() throws Exception {
    createBQTableWith(
        new TableRow().set("id", 1).set("name", "name1"),
        new TableRow().set("id", 2).set("name", "name2"),
        new TableRow().set("id", 3).set("name", "name3"));

    TableReference bqTable = bigQuery.tableReference();
    String tableId =
        String.format(
            "bigquery.`table`.`%s`.`%s`.`%s`",
            bqTable.getProjectId(), bqTable.getDatasetId(), bqTable.getTableId());

    PCollection<Row> result =
        readPipeline.apply(
            "query",
            SqlTransform.query("SELECT id, name FROM " + tableId)
                .withDefaultTableProvider(
                    "datacatalog", DataCatalogTableProvider.create(readPipeline.getOptions())));

    PAssert.that(result).containsInAnyOrder(row(1, "name1"), row(2, "name2"), row(3, "name3"));
    readPipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  private Row row(long id, String name) {
    return Row.withSchema(ID_NAME_SCHEMA).addValues(id, name).build();
  }

  private void createBQTableWith(TableRow r1, TableRow r2, TableRow r3) {
    writeToBQPipeline
        .apply(Create.of(r1, r2, r3).withCoder(TableRowJsonCoder.of()))
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
    writeToBQPipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }
}
