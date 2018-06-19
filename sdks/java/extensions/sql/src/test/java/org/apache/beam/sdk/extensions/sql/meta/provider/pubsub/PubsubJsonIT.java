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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for querying Pubsub JSON messages with SQL. */
@RunWith(JUnit4.class)
public class PubsubJsonIT implements Serializable {

  private static final Schema PAYLOAD_SCHEMA =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT32)
          .addNullableField("name", Schema.FieldType.STRING)
          .build();

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
