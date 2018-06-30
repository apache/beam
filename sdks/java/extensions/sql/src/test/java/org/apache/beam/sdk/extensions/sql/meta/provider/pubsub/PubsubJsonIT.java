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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.common.ReflectHelpers;
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

  /**
   * HACK: we need an objectmapper to turn pipelineoptions back into a map. We need to use
   * ReflectHelpers to get the extra PipelineOptions.
   */
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

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

    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(5));
    pipeline.begin().apply(signal.signalStart());
    pipeline.run();
    start.get();

    eventsTopic.publish(messages);
    signal.waitForSuccess(Duration.standardSeconds(60));
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
        pipeline.apply(
            PubsubIO.readMessagesWithAttributes().fromTopic(dlqTopic.topicPath().getPath()));

    dlq.apply(
        "waitForDlq",
        signal.signalSuccessWhen(
            PubsubMessageWithAttributesCoder.of(),
            dlqMessages ->
                containsAll(dlqMessages, message(ts(4), "{ - }"), message(ts(5), "{ + }"))));

    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(5));
    pipeline.begin().apply(signal.signalStart());
    pipeline.run();
    start.get();

    eventsTopic.publish(messages);
    signal.waitForSuccess(Duration.standardSeconds(60));
  }

  @Test
  public void testSQLLimit() throws Exception {
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

    // We need the default options on the schema to include the project passed in for the integration test
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

    // wait one minute to allow subscription creation.
    Thread.sleep(60 * 1000);
    eventsTopic.publish(messages);
    assertThat(queryResult.get().size(), equalTo(3));
    pool.shutdown();
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
    Map<String, Object> optionsMap =
        (Map<String, Object>) MAPPER.convertValue(pipeline.getOptions(), Map.class).get("options");
    Map<String, String> argsMap =
        optionsMap
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> toArg(entry.getValue())));

    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      inMemoryMetaStore.registerProvider(tableProvider);
    }

    Properties info = new Properties();
    BeamCalciteSchema dbSchema = new BeamCalciteSchema(inMemoryMetaStore);
    dbSchema.getPipelineOptions().putAll(argsMap);
    info.put(BEAM_CALCITE_SCHEMA, dbSchema);
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
