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

import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARBINARY;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter.Operator;
import com.google.datastore.v1.Query;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.io.gcp.datastore.EntityToRow;
import org.apache.beam.sdk.io.gcp.datastore.RowToEntity;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.ByteString;
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
      Schema.builder()
          .addNullableField("__key__", VARBINARY)
          .addNullableField("content", STRING)
          .build();
  private static final Schema SOURCE_SCHEMA_WITHOUT_KEY =
      Schema.builder().addNullableField("content", STRING).build();
  private static final String KIND = "writereadtest";
  private static final String KIND_ALL_TYPES = "writereadalltypestest";

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Test
  public void testDataStoreV1SqlWriteRead() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new DataStoreV1TableProvider());
    String projectId = options.getProject();

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   `__key__` VARBINARY, \n"
            + "   `content` VARCHAR \n"
            + ") \n"
            + "TYPE 'datastoreV1' \n"
            + "LOCATION '"
            + projectId
            + "/"
            + KIND
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    Key ancestor = makeKey(KIND, UUID.randomUUID().toString()).build();
    Key itemKey = makeKey(ancestor, KIND, UUID.randomUUID().toString()).build();
    String insertStatement =
        "INSERT INTO TEST VALUES ( \n" + keyToSqlByteString(itemKey) + ", \n" + "'2000' \n" + ")";

    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(insertStatement));
    writePipeline.run().waitUntilFinish();

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    assertThat(output.getSchema(), equalTo(SOURCE_SCHEMA));

    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testDataStoreV1SqlWriteRead_withoutKey() {
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

    String insertStatement = "INSERT INTO TEST VALUES ( '3000' )";

    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(insertStatement));
    writePipeline.run().waitUntilFinish();

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    assertThat(output.getSchema(), equalTo(SOURCE_SCHEMA_WITHOUT_KEY));

    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testWriteRead_viaCoreBeamIO() {
    String projectId = options.getProject();
    Key ancestor = makeKey(KIND, UUID.randomUUID().toString()).build();
    Key itemKey =
        makeKey(ancestor, KIND, UUID.randomUUID().toString())
            .setPartitionId(PartitionId.newBuilder().setProjectId(projectId).build())
            .build();
    Row testWriteRow =
        Row.withSchema(SOURCE_SCHEMA).addValues(itemKey.toByteArray(), "4000").build();

    writePipeline
        .apply(Create.of(testWriteRow).withRowSchema(SOURCE_SCHEMA))
        .apply(RowToEntity.create("__key__", KIND))
        .apply(DatastoreIO.v1().write().withProjectId(projectId));
    writePipeline.run().waitUntilFinish();

    Query.Builder query = Query.newBuilder();
    query.addKindBuilder().setName(KIND);
    query.setFilter(makeFilter("__key__", Operator.EQUAL, makeValue(itemKey)));

    DatastoreV1.Read read =
        DatastoreIO.v1().read().withProjectId(projectId).withQuery(query.build());
    PCollection<Row> rowsRead =
        readPipeline.apply(read).apply(EntityToRow.create(SOURCE_SCHEMA, "__key__"));

    PAssert.that(rowsRead).containsInAnyOrder(testWriteRow);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadAllSupportedTypes() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new DataStoreV1TableProvider());
    String projectId = options.getProject();

    final Schema expectedSchema =
        Schema.builder()
            .addNullableField("__key__", VARBINARY)
            .addNullableField("boolean", BOOLEAN)
            .addNullableField("datetime", DATETIME)
            // TODO: flattening of nested fields by Calcite causes some issues.
            /*.addRowField("embeddedentity",
            Schema.builder()
                .addNullableField("property1", STRING)
                .addNullableField("property2", INT64)
                .build())*/
            .addNullableField("floatingnumber", DOUBLE)
            .addNullableField("integer", INT64)
            .addNullableField("primitivearray", FieldType.array(STRING))
            .addNullableField("string", STRING)
            .addNullableField("text", STRING)
            .build();

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   `__key__` VARBINARY, \n"
            + "   `boolean` BOOLEAN, \n"
            + "   `datetime` TIMESTAMP, \n"
            // + "   `embeddedentity` ROW(`property1` VARCHAR, `property2` BIGINT), \n"
            + "   `floatingnumber` DOUBLE, \n"
            + "   `integer` BIGINT, \n"
            + "   `primitivearray` ARRAY<VARCHAR>, \n"
            + "   `string` VARCHAR, \n"
            + "   `text` VARCHAR"
            + ") \n"
            + "TYPE 'datastoreV1' \n"
            + "LOCATION '"
            + projectId
            + "/"
            + KIND_ALL_TYPES
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    assertThat(output.getSchema(), equalTo(expectedSchema));

    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  private static String keyToSqlByteString(Key key) {
    return "X'" + ByteString.toString(key.toByteArray(), 16) + "'";
  }
}
