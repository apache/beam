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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for writing to Iceberg with Beam SQL. */
@RunWith(JUnit4.class)
public class IcebergReadWriteIT {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("c_bigint", INT64)
          .addNullableField("c_integer", INT32)
          .addNullableField("c_float", FLOAT)
          .addNullableField("c_double", DOUBLE)
          .addNullableField("c_boolean", BOOLEAN)
          .addNullableField("c_timestamp", CalciteUtils.TIMESTAMP)
          .addNullableField("c_varchar", STRING)
          .addNullableField("c_char", STRING)
          .addNullableField("c_arr", Schema.FieldType.array(STRING))
          .build();

  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("IcebergReadWriteIT");
  static final String DATASET = "iceberg_sql_tests_" + System.nanoTime();
  static String warehouse;
  protected static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);

  @BeforeClass
  public static void createDataset() throws IOException, InterruptedException {
    warehouse =
        String.format(
            "%s%s/%s",
            TestPipeline.testingPipelineOptions().getTempLocation(),
            IcebergReadWriteIT.class.getSimpleName(),
            UUID.randomUUID());
    BQ_CLIENT.createNewDataset(OPTIONS.getProject(), DATASET);
  }

  @AfterClass
  public static void deleteDataset() {
    BQ_CLIENT.deleteDataset(OPTIONS.getProject(), DATASET);
  }

  @Test
  public void testSqlWriteAndRead() throws IOException, InterruptedException {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new IcebergTableProvider());
    String tableIdentifier = DATASET + ".my_table";

    // 1) create beam table
    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'iceberg' \n"
            + "LOCATION '"
            + tableIdentifier
            + "'\n"
            + "TBLPROPERTIES '{\"catalog_properties\": {"
            + "\"catalog-impl\": \"org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog\","
            + "\"io-impl\": \"org.apache.iceberg.gcp.gcs.GCSFileIO\","
            + "\"warehouse\": \""
            + warehouse
            + "\","
            + "\"gcp_project\": \""
            + OPTIONS.getProject()
            + "\","
            + "\"gcp_region\": \"us-central1\""
            + "}}'";
    sqlEnv.executeDdl(createTableStatement);

    // 2) write to underlying Iceberg table
    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";
    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(insertStatement));
    writePipeline.run().waitUntilFinish();

    // 3) run external query on Iceberg table (hosted on BQ) to verify correct row was written
    String query = String.format("SELECT * FROM `%s.%s`", OPTIONS.getProject(), tableIdentifier);
    TableRow returnedRow =
        BQ_CLIENT.queryUnflattened(query, OPTIONS.getProject(), true, true).get(0);
    Row beamRow = BigQueryUtils.toBeamRow(SOURCE_SCHEMA, returnedRow);
    Row expectedRow =
        Row.withSchema(SOURCE_SCHEMA)
            .addValues(
                9223372036854775807L,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                Arrays.asList("123", "456"))
            .build();
    assertEquals(expectedRow, beamRow);

    // 4) read using Beam SQL and verify
    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));
    PAssert.that(output).containsInAnyOrder(expectedRow);
    PipelineResult.State state = readPipeline.run().waitUntilFinish();
    assertThat(state, equalTo(PipelineResult.State.DONE));
  }
}
