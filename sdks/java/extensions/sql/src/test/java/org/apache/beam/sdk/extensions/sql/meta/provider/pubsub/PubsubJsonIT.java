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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for querying Pubsub JSON messages with SQL.
 */
@RunWith(JUnit4.class)
public class PubsubJsonIT implements Serializable {

  private static final Schema PAYLOAD_SCHEMA =
      RowSqlTypes
          .builder()
          .withIntegerField("id")
          .withVarcharField("name")
          .build();

  @Rule public transient TestPubsub pubsub = TestPubsub.create();
  @Rule public transient TestPubsubSignal signal = TestPubsubSignal.create();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSelectsPayloadContent() throws Exception {
    String createTableString =
        "CREATE TABLE message (\n"
        + "event_timestamp TIMESTAMP, \n"
        + "attributes MAP<VARCHAR, VARCHAR>, \n"
        + "payload ROW< \n"
        + "             id INTEGER, \n"
        + "             name VARCHAR \n"
        + "           > \n"
        + ") \n"
        + "TYPE 'pubsub' \n"
        + "LOCATION '" + pubsub.evevntsTopicPath() + "' \n"
        + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";

    String queryString = "SELECT message.payload.id, message.payload.name from message";

    List<PubsubMessage> messages = ImmutableList.of(
        message(ts(1), 3, "foo"),
        message(ts(2), 5, "bar"),
        message(ts(3), 7, "baz"));

    BeamSqlEnv sqlEnv = newSqlEnv();

    createTable(sqlEnv, createTableString);
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    queryOutput
        .apply(
            "waitForSuccess",
            signal.signalSuccessWhen(
                PAYLOAD_SCHEMA.getRowCoder(),
                observedRows -> observedRows.equals(
                    ImmutableSet.of(
                        row(PAYLOAD_SCHEMA, 3, "foo"),
                        row(PAYLOAD_SCHEMA, 5, "bar"),
                        row(PAYLOAD_SCHEMA, 7, "baz")))));

    pipeline.run();
    pubsub.publish(messages);
    signal.waitForSuccess(Duration.standardSeconds(120));
  }

  private BeamSqlEnv newSqlEnv() {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new PubsubJsonTableProvider());
    return new BeamSqlEnv(metaStore);
  }

  private void createTable(BeamSqlEnv sqlEnv, String statement) throws SqlParseException {
    SqlNode sqlNode = sqlEnv.getPlanner().parse(statement);
    ((SqlExecutableStatement) sqlNode).execute(sqlEnv.getContext());
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  private PCollection<Row> query(
      BeamSqlEnv sqlEnv,
      TestPipeline pipeline,
      String queryString)
      throws Exception {

    return sqlEnv.getPlanner().compileBeamPipeline(queryString, pipeline);
  }

  private PubsubMessage message(Instant timestamp, int id, String name) {
    return
        new PubsubMessage(
            jsonString(id, name).getBytes(UTF_8),
            ImmutableMap.of("ts", String.valueOf(timestamp.getMillis())));
  }

  private String jsonString(int id, String name) {
    return "{ \"id\" : " + id + ", \"name\" : \"" + name + "\" }";
  }

  private Instant ts(long millis) {
    return new Instant(millis);
  }
}
