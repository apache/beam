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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.jdbc.CalciteConnection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for querying Pubsub JSON messages with SQL. */
@RunWith(JUnit4.class)
public class PubsubJsonIT implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubJsonIT.class);

  private static final Schema PAYLOAD_SCHEMA =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT32)
          .addNullableField("name", Schema.FieldType.STRING)
          .build();

  private static final String CONNECT_STRING_PREFIX = "jdbc:beam:";
  private static final String BEAM_CALCITE_SCHEMA = "beamCalciteSchema";
  private static final JdbcDriver INSTANCE = new JdbcDriver();
  private static volatile Boolean checked = false;

  @Rule public transient TestPubsub eventsTopic = TestPubsub.create();
  @Rule public transient TestPubsub dlqTopic = TestPubsub.create();
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
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";

    String queryString = "SELECT message.payload.id, message.payload.name from message";

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());

    sqlEnv.executeDdl(createTableString);
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    queryOutput.apply(
        "waitForSuccess",
        signal.signalSuccessWhen(
            PAYLOAD_SCHEMA.getRowCoder(),
            observedRows ->
                observedRows.equals(
                    ImmutableSet.of(
                        row(PAYLOAD_SCHEMA, 3, "foo"),
                        row(PAYLOAD_SCHEMA, 5, "bar"),
                        row(PAYLOAD_SCHEMA, 7, "baz")))));

    pipeline.run();
    eventsTopic.publish(messages);
    signal.waitForSuccess(Duration.standardMinutes(5));
  }

  @Test
  public void testUsesDlq() throws Exception {
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
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n"
            + "TBLPROPERTIES "
            + "    '{ "
            + "       \"timestampAttributeKey\" : \"ts\", "
            + "       \"deadLetterQueue\" : \""
            + dlqTopic.topicPath()
            + "\""
            + "     }'";

    String queryString = "SELECT message.payload.id, message.payload.name from message";

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"),
            message(ts(2), 5, "bar"),
            message(ts(3), 7, "baz"),
            message(ts(4), "{ - }"), // invalid
            message(ts(5), "{ + }")); // invalid

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());

    sqlEnv.executeDdl(createTableString);
    query(sqlEnv, pipeline, queryString);
    PCollection<PubsubMessage> dlq =
        pipeline.apply(PubsubIO.readMessagesWithAttributes().fromTopic(dlqTopic.topicPath()));

    dlq.apply(
        "waitForDlq",
        signal.signalSuccessWhen(
            PubsubMessageWithAttributesCoder.of(),
            dlqMessages ->
                containsAll(dlqMessages, message(ts(4), "{ - }"), message(ts(5), "{ + }"))));

    pipeline.run();

    eventsTopic.publish(messages);
    signal.waitForSuccess(Duration.standardMinutes(5));
  }

  @Test
  public void testSQLLimit() throws SQLException, IOException, InterruptedException {
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
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n"
            + "TBLPROPERTIES "
            + "    '{ "
            + "       \"timestampAttributeKey\" : \"ts\", "
            + "       \"deadLetterQueue\" : \""
            + dlqTopic.topicPath()
            + "\""
            + "     }'";

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"),
            message(ts(2), 5, "bar"),
            message(ts(3), 7, "baz"),
            message(ts(4), 9, "ba2"),
            message(ts(5), 10, "ba3"),
            message(ts(6), 13, "ba4"),
            message(ts(7), 15, "ba5"));

    CalciteConnection connection = connect(new PubsubJsonTableProvider());

    Statement statement = connection.createStatement();
    statement.execute(createTableString);

    // Because Pubsub only allow new subscription receives message after the subscription is
    // created, eventsTopic.publish(messages) can only be called after statement.executeQuery.
    // However, because statement.executeQuery is a blocking call, it has to be put into a
    // seperate thread to execute.
    ExecutorService pool = Executors.newFixedThreadPool(1);
    pool.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              ResultSet resultSet =
                  statement.executeQuery("SELECT message.payload.id FROM message LIMIT 3");
              assertTrue(resultSet.next());
              assertTrue(resultSet.next());
              assertTrue(resultSet.next());
              assertFalse(resultSet.next());
              checked = true;
            } catch (SQLException e) {
              LOG.warn(e.toString());
            }
          }
        });

    // wait one minute to allow subscription creation.
    Thread.sleep(60 * 1000);
    eventsTopic.publish(messages);
    // Wait one minute to allow the thread finishes checks.
    Thread.sleep(60 * 1000);
    // verify if the thread has checked returned value from LIMIT query.
    assertTrue(checked);
    pool.shutdown();
  }

  private CalciteConnection connect(TableProvider... tableProviders) throws SQLException {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      inMemoryMetaStore.registerProvider(tableProvider);
    }

    Properties info = new Properties();
    info.put(BEAM_CALCITE_SCHEMA, new BeamCalciteSchema(inMemoryMetaStore));
    return (CalciteConnection) INSTANCE.connect(CONNECT_STRING_PREFIX, info);
  }

  private static Boolean containsAll(Set<PubsubMessage> set, PubsubMessage... subsetCandidate) {
    return Arrays.stream(subsetCandidate)
        .allMatch(candidate -> set.stream().anyMatch(element -> messagesEqual(element, candidate)));
  }

  private static boolean messagesEqual(PubsubMessage message1, PubsubMessage message2) {
    return message1.getAttributeMap().equals(message2.getAttributeMap())
        && Arrays.equals(message1.getPayload(), message2.getPayload());
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  private PCollection<Row> query(BeamSqlEnv sqlEnv, TestPipeline pipeline, String queryString)
      throws Exception {

    return BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(queryString));
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
