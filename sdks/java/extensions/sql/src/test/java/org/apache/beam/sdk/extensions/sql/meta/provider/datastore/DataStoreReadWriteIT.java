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
package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataStoreReadWriteIT {
  private static final BigQueryOptions options =
      TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
  private static final Schema SOURCE_SCHEMA =
      Schema.builder().addNullableField("content", STRING).build();
  private static final String KIND = "writereadtest";

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Test
  public void testDataStoreV1SqlWriteRead() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new DataStoreV1TableProvider());
    String projectId = options.getProject();

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   `content` VARCHAR \n"
            + ") \n"
            + "TYPE 'datastoreV1' \n"
            + "LOCATION '"
            + projectId
            + "/"
            + KIND
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement = "INSERT INTO TEST VALUES (" + "'varchar'" + ")";

    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(insertStatement));
    writePipeline.run().waitUntilFinish();

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    assertThat(output.getSchema(), equalTo(SOURCE_SCHEMA));

    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }
}
