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
package org.apache.beam.sdk.extensions.sql;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests form writing to BigQuery with Beam SQL. */
@RunWith(JUnit4.class)
public class PubsubToBigqueryIT implements Serializable {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder().addNullableField("id", INT64).addNullableField("name", STRING).build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestPubsub pubsub = TestPubsub.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(SOURCE_SCHEMA);

  @Test
  public void testSimpleInsert() throws Exception {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubTableProvider(), new BigQueryTableProvider());

    String createTableString =
        "CREATE EXTERNAL TABLE pubsub_topic (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "attributes MAP<VARCHAR, VARCHAR>, \n"
            + "payload ROW< \n"
            + "             id INTEGER, \n"
            + "             name VARCHAR \n"
            + "           > \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + pubsub.topicPath()
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";
    sqlEnv.executeDdl(createTableString);

    String createTableStatement =
        "CREATE EXTERNAL TABLE bq_table( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR \n "
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO bq_table \n"
            + "SELECT \n"
            + "  pubsub_topic.payload.id, \n"
            + "  pubsub_topic.payload.name \n"
            + "FROM pubsub_topic";

    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));

    pipeline.run();

    // Block until a subscription for this topic exists
    pubsub.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));
    pubsub.publish(messages);

    bigQuery
        .assertThatAllRows(SOURCE_SCHEMA)
        .eventually(
            containsInAnyOrder(
                row(SOURCE_SCHEMA, 3L, "foo"),
                row(SOURCE_SCHEMA, 5L, "bar"),
                row(SOURCE_SCHEMA, 7L, "baz")))
        .pollFor(Duration.standardMinutes(5));
  }

  @Test
  public void testSimpleInsertFlat() throws Exception {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubTableProvider(), new BigQueryTableProvider());

    String createTableString =
        "CREATE EXTERNAL TABLE pubsub_topic (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "id INTEGER, \n"
            + "name VARCHAR \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + pubsub.topicPath()
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";
    sqlEnv.executeDdl(createTableString);

    String createTableStatement =
        "CREATE EXTERNAL TABLE bq_table( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR \n "
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO bq_table \n" + "SELECT \n" + "  id, \n" + "  name \n" + "FROM pubsub_topic";

    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));

    pipeline.run();

    // Block until a subscription for this topic exists
    pubsub.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));
    pubsub.publish(messages);

    bigQuery
        .assertThatAllRows(SOURCE_SCHEMA)
        .eventually(
            containsInAnyOrder(
                row(SOURCE_SCHEMA, 3L, "foo"),
                row(SOURCE_SCHEMA, 5L, "bar"),
                row(SOURCE_SCHEMA, 7L, "baz")))
        .pollFor(Duration.standardMinutes(5));
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  private PubsubMessage message(Instant timestamp, int id, String name) {
    return message(timestamp, jsonString(id, name));
  }

  private PubsubMessage message(Instant timestamp, String jsonPayload) {
    return new PubsubMessage(
        jsonPayload.getBytes(UTF_8), ImmutableMap.of("ts", String.valueOf(timestamp.getMillis())));
  }

  private String jsonString(int id, String name) {
    return "{ \"id\" : " + id + ", \"name\" : \"" + name + "\" }";
  }

  private Instant ts(long millis) {
    return new Instant(millis);
  }
}
