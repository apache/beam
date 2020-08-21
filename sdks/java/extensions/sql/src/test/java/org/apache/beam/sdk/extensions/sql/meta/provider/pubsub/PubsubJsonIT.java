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
import static org.apache.beam.sdk.testing.JsonMatcher.jsonBytesLike;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteConnection;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
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
  @Rule public transient TestPubsub filteredEventsTopic = TestPubsub.create();
  @Rule public transient TestPubsub dlqTopic = TestPubsub.create();
  @Rule public transient TestPubsubSignal resultSignal = TestPubsubSignal.create();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestPipeline filterPipeline = TestPipeline.create();

  /**
   * HACK: we need an objectmapper to turn pipelineoptions back into a map. We need to use
   * ReflectHelpers to get the extra PipelineOptions.
   */
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  @Test
  public void testSQLSelectsPayloadContent() throws Exception {
    String createTableString =
        "CREATE EXTERNAL TABLE message (\n"
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

    // Prepare messages to send later
    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));

    // Initialize SQL environment and create the pubsub table
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());
    sqlEnv.executeDdl(createTableString);

    // Apply the PTransform to query the pubsub topic
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    // Observe the query results and send success signal after seeing the expected messages
    queryOutput.apply(
        "waitForSuccess",
        resultSignal.signalSuccessWhen(
            SchemaCoder.of(PAYLOAD_SCHEMA),
            observedRows ->
                observedRows.equals(
                    ImmutableSet.of(
                        row(PAYLOAD_SCHEMA, 3, "foo"),
                        row(PAYLOAD_SCHEMA, 5, "bar"),
                        row(PAYLOAD_SCHEMA, 7, "baz")))));

    // Start the pipeline
    pipeline.run();

    // Block until a subscription for this topic exists
    eventsTopic.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    // Start publishing the messages when main pipeline is started and signaling topic is ready
    eventsTopic.publish(messages);

    // Poll the signaling topic for success message
    resultSignal.waitForSuccess(Duration.standardMinutes(5));
  }

  @Ignore("Disable flake tracked at https://issues.apache.org/jira/browse/BEAM-5122")
  @Test
  public void testUsesDlq() throws Exception {
    String createTableString =
        "CREATE EXTERNAL TABLE message (\n"
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

    // Prepare messages to send later
    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"),
            message(ts(2), 5, "bar"),
            message(ts(3), 7, "baz"),
            message(ts(4), "{ - }"), // invalid message, will go to DLQ
            message(ts(5), "{ + }")); // invalid message, will go to DLQ

    // Initialize SQL environment and create the pubsub table
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());
    sqlEnv.executeDdl(createTableString);

    // Apply the PTransform to query the pubsub topic
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    // Observe the query results and send success signal after seeing the expected messages
    queryOutput.apply(
        "waitForSuccess",
        resultSignal.signalSuccessWhen(
            SchemaCoder.of(PAYLOAD_SCHEMA),
            observedRows ->
                observedRows.equals(
                    ImmutableSet.of(
                        row(PAYLOAD_SCHEMA, 3, "foo"),
                        row(PAYLOAD_SCHEMA, 5, "bar"),
                        row(PAYLOAD_SCHEMA, 7, "baz")))));

    // Start the pipeline
    pipeline.run();

    // Block until a subscription for this topic exists
    eventsTopic.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    // Start publishing the messages when main pipeline is started and signaling topics are ready
    eventsTopic.publish(messages);

    // Poll the signaling topic for success message
    resultSignal.waitForSuccess(Duration.standardMinutes(2));
    dlqTopic
        .assertThatTopicEventuallyReceives(messageLike(ts(4), "{ - }"), messageLike(ts(5), "{ + }"))
        .waitForUpTo(Duration.standardSeconds(20));
  }

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-7582")
  public void testSQLLimit() throws Exception {
    String createTableString =
        "CREATE EXTERNAL TABLE message (\n"
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

    // We need the default options on the schema to include the project passed in for the
    // integration test
    CalciteConnection connection = connect(pipeline.getOptions(), new PubsubJsonTableProvider());

    Statement statement = connection.createStatement();
    statement.execute(createTableString);

    // Because Pubsub only allow new subscription receives message after the subscription is
    // created, eventsTopic.publish(messages) can only be called after statement.executeQuery.
    // However, because statement.executeQuery is a blocking call, it has to be put into a
    // seperate thread to execute.
    ExecutorService pool = Executors.newFixedThreadPool(1);
    Future<List<String>> queryResult =
        pool.submit(
            (Callable)
                () -> {
                  ResultSet resultSet =
                      statement.executeQuery("SELECT message.payload.id FROM message LIMIT 3");
                  ImmutableList.Builder<String> result = ImmutableList.builder();
                  while (resultSet.next()) {
                    result.add(resultSet.getString(1));
                  }
                  return result.build();
                });

    eventsTopic.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));
    eventsTopic.publish(messages);
    assertThat(queryResult.get(2, TimeUnit.MINUTES).size(), equalTo(3));
    pool.shutdown();
  }

  @Test
  public void testWritesJsonRowsToPubsub() throws Exception {
    Schema personSchema =
        Schema.builder()
            .addStringField("name")
            .addInt32Field("height")
            .addBooleanField("knowsJavascript")
            .build();
    PCollection<Row> rows =
        pipeline
            .apply(
                Create.of(
                    row(personSchema, "person1", 80, true),
                    row(personSchema, "person2", 70, false),
                    row(personSchema, "person3", 60, true),
                    row(personSchema, "person4", 50, false),
                    row(personSchema, "person5", 40, true)))
            .setRowSchema(personSchema)
            .apply(
                SqlTransform.query(
                    "SELECT name FROM PCOLLECTION AS person WHERE person.knowsJavascript"));

    // Convert rows to JSON and write to pubsub
    rows.apply(ToJson.of()).apply(PubsubIO.writeStrings().to(eventsTopic.topicPath().getPath()));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    eventsTopic
        .assertThatTopicEventuallyReceives(
            messageLike("{\"name\":\"person1\"}"),
            messageLike("{\"name\":\"person3\"}"),
            messageLike("{\"name\":\"person5\"}"))
        .waitForUpTo(Duration.standardSeconds(20));
  }

  @Test
  public void testSQLSelectsPayloadContentFlat() throws Exception {
    String createTableString =
        "CREATE EXTERNAL TABLE message (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "id INTEGER, \n"
            + "name VARCHAR \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";

    String queryString = "SELECT message.id, message.name from message";

    // Prepare messages to send later
    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));

    // Initialize SQL environment and create the pubsub table
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());
    sqlEnv.executeDdl(createTableString);

    // Apply the PTransform to query the pubsub topic
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    // Observe the query results and send success signal after seeing the expected messages
    queryOutput.apply(
        "waitForSuccess",
        resultSignal.signalSuccessWhen(
            SchemaCoder.of(PAYLOAD_SCHEMA),
            observedRows ->
                observedRows.equals(
                    ImmutableSet.of(
                        row(PAYLOAD_SCHEMA, 3, "foo"),
                        row(PAYLOAD_SCHEMA, 5, "bar"),
                        row(PAYLOAD_SCHEMA, 7, "baz")))));

    // Start the pipeline
    pipeline.run();

    // Block until a subscription for this topic exists
    eventsTopic.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    // Start publishing the messages when main pipeline is started and signaling topic is ready
    eventsTopic.publish(messages);

    // Poll the signaling topic for success message
    resultSignal.waitForSuccess(Duration.standardMinutes(5));
  }

  @Test
  public void testSQLInsertJsonRowsToPubsubFlat() throws Exception {
    String createTableString =
        "CREATE EXTERNAL TABLE message (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "name VARCHAR, \n"
            + "height INTEGER, \n"
            + "knowsJavascript BOOLEAN \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n"
            + "TBLPROPERTIES "
            + "    '{ "
            + "       \"deadLetterQueue\" : \""
            + dlqTopic.topicPath()
            + "\""
            + "     }'";

    // Initialize SQL environment and create the pubsub table
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());
    sqlEnv.executeDdl(createTableString);

    // TODO(BEAM-8741): Ideally we could write this query without specifying a column list, because
    //   it shouldn't be possible to write to event_timestamp when it's mapped to  publish time.
    String queryString =
        "INSERT INTO message (name, height, knowsJavascript) \n"
            + "VALUES \n"
            + "('person1', 80, TRUE), \n"
            + "('person2', 70, FALSE)";

    // Apply the PTransform to insert the rows
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    eventsTopic
        .assertThatTopicEventuallyReceives(
            jsonMessageLike("{\"name\":\"person1\", \"height\": 80, \"knowsJavascript\": true}"),
            jsonMessageLike("{\"name\":\"person2\", \"height\": 70, \"knowsJavascript\": false}"))
        .waitForUpTo(Duration.standardSeconds(20));
  }

  @Test
  public void testSQLInsertJsonRowsToPubsubWithTimestampAttributeFlat() throws Exception {
    String createTableString =
        "CREATE EXTERNAL TABLE message (\n"
            + "  event_timestamp TIMESTAMP, \n"
            + "  name VARCHAR, \n"
            + "  height INTEGER, \n"
            + "  knowsJavascript BOOLEAN \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n"
            + "TBLPROPERTIES "
            + "  '{ "
            + "     \"deadLetterQueue\" : \""
            + dlqTopic.topicPath()
            + "\","
            + "     \"timestampAttributeKey\" : \"ts\""
            + "   }'";

    // Initialize SQL environment and create the pubsub table
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());
    sqlEnv.executeDdl(createTableString);

    String queryString =
        "INSERT INTO message "
            + "VALUES "
            + "(TIMESTAMP '1970-01-01 00:00:00.001', 'person1', 80, TRUE), "
            + "(TIMESTAMP '1970-01-01 00:00:00.002', 'person2', 70, FALSE)";
    PCollection<Row> queryOutput = query(sqlEnv, pipeline, queryString);

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    eventsTopic
        .assertThatTopicEventuallyReceives(
            jsonMessageLike(
                ts(1), "{\"name\":\"person1\", \"height\": 80, \"knowsJavascript\": true}"),
            jsonMessageLike(
                ts(2), "{\"name\":\"person2\", \"height\": 70, \"knowsJavascript\": false}"))
        .waitForUpTo(Duration.standardSeconds(20));
  }

  @Test
  public void testSQLReadAndWriteWithSameFlatTableDefinition() throws Exception {
    // This test verifies that the same pubsub table definition can be used for both reading and
    // writing
    // pipeline: Use SQL to insert data into `people`
    // filterPipeline: Use SQL to read from `people`, filter the rows, and write to
    // `javascript_people`

    String createTableString =
        "CREATE EXTERNAL TABLE people (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "name VARCHAR, \n"
            + "height INTEGER, \n"
            + "knowsJavascript BOOLEAN \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + eventsTopic.topicPath()
            + "' \n";

    String createFilteredTableString =
        "CREATE EXTERNAL TABLE javascript_people (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "name VARCHAR, \n"
            + "height INTEGER \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + filteredEventsTopic.topicPath()
            + "' \n";

    // Initialize SQL environment and create the pubsub table
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new PubsubJsonTableProvider());
    sqlEnv.executeDdl(createTableString);
    sqlEnv.executeDdl(createFilteredTableString);

    // TODO(BEAM-8741): Ideally we could write these queries without specifying a column list,
    // because
    //   it shouldn't be possible to write to event_timestamp when it's mapped to  publish time.
    String filterQueryString =
        "INSERT INTO javascript_people (name, height) (\n"
            + "  SELECT \n"
            + "    name, \n"
            + "    height \n"
            + "  FROM people \n"
            + "  WHERE knowsJavascript \n"
            + ")";

    String injectQueryString =
        "INSERT INTO people (name, height, knowsJavascript) VALUES \n"
            + "('person1', 80, TRUE),  \n"
            + "('person2', 70, FALSE), \n"
            + "('person3', 60, TRUE),  \n"
            + "('person4', 50, FALSE), \n"
            + "('person5', 40, TRUE)";

    // Apply the PTransform to do the filtering
    query(sqlEnv, filterPipeline, filterQueryString);

    // Apply the PTransform to inject the input data
    query(sqlEnv, pipeline, injectQueryString);

    // Start the filter pipeline and wait until it has started.
    filterPipeline.run();

    // Block until a subscription for this topic exists
    eventsTopic.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    // .. then run the injector pipeline
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    filteredEventsTopic
        .assertThatTopicEventuallyReceives(
            jsonMessageLike("{\"name\":\"person1\", \"height\": 80}"),
            jsonMessageLike("{\"name\":\"person3\", \"height\": 60}"),
            jsonMessageLike("{\"name\":\"person5\", \"height\": 40}"))
        .waitForUpTo(Duration.standardMinutes(5));
  }

  private static String toArg(Object o) {
    try {
      String jsonRepr = MAPPER.writeValueAsString(o);

      // String and enums are expected to be unquoted on the command line
      if (jsonRepr.startsWith("\"") && jsonRepr.endsWith("\"")) {
        return jsonRepr.substring(1, jsonRepr.length() - 1);
      } else {
        return jsonRepr;
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private CalciteConnection connect(PipelineOptions options, TableProvider... tableProviders)
      throws SQLException {
    // HACK: PipelineOptions should expose a prominent method to do this reliably
    // The actual options are in the "options" field of the converted map
    Map<String, String> argsMap =
        ((Map<String, Object>) MAPPER.convertValue(pipeline.getOptions(), Map.class).get("options"))
            .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> toArg(entry.getValue())));

    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      inMemoryMetaStore.registerProvider(tableProvider);
    }

    JdbcConnection connection = JdbcDriver.connect(inMemoryMetaStore, options);
    connection.setPipelineOptionsMap(argsMap);
    return connection;
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

  private Matcher<PubsubMessage> messageLike(Instant timestamp, String jsonPayload) {
    return allOf(
        hasProperty("payload", equalTo(jsonPayload.getBytes(StandardCharsets.US_ASCII))),
        hasProperty("attributeMap", hasEntry("ts", String.valueOf(timestamp.getMillis()))));
  }

  private Matcher<PubsubMessage> messageLike(String jsonPayload) {
    return hasProperty("payload", equalTo(jsonPayload.getBytes(StandardCharsets.US_ASCII)));
  }

  private Matcher<PubsubMessage> jsonMessageLike(Instant timestamp, String jsonPayload)
      throws IOException {
    return allOf(
        hasProperty("payload", jsonBytesLike(jsonPayload)),
        hasProperty("attributeMap", hasEntry("ts", String.valueOf(timestamp.getMillis()))));
  }

  private Matcher<PubsubMessage> jsonMessageLike(String jsonPayload) throws IOException {
    return hasProperty("payload", jsonBytesLike(jsonPayload));
  }

  private String jsonString(int id, String name) {
    return "{ \"id\" : " + id + ", \"name\" : \"" + name + "\" }";
  }

  private Instant ts(long millis) {
    return new Instant(millis);
  }
}
