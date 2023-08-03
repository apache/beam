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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PubsubLiteTableProviderTest {
  private static final PubsubLiteTableProvider PROVIDER = new PubsubLiteTableProvider();
  private static final Schema FULL_WRITE_SCHEMA =
      Schema.builder()
          .addByteArrayField(RowHandler.MESSAGE_KEY_FIELD)
          .addDateTimeField(RowHandler.EVENT_TIMESTAMP_FIELD)
          .addArrayField(
              RowHandler.ATTRIBUTES_FIELD, FieldType.row(RowHandler.ATTRIBUTES_ENTRY_SCHEMA))
          .addByteArrayField(RowHandler.PAYLOAD_FIELD)
          .build();
  private static final Schema FULL_READ_SCHEMA =
      Schema.builder()
          .addByteArrayField(RowHandler.MESSAGE_KEY_FIELD)
          .addDateTimeField(RowHandler.EVENT_TIMESTAMP_FIELD)
          .addArrayField(
              RowHandler.ATTRIBUTES_FIELD, FieldType.row(RowHandler.ATTRIBUTES_ENTRY_SCHEMA))
          .addByteArrayField(RowHandler.PAYLOAD_FIELD)
          .addDateTimeField(RowHandler.PUBLISH_TIMESTAMP_FIELD)
          .build();

  private static BeamSqlTable makeTable(
      Schema schema, String location, Map<String, Object> properties) {
    Table table =
        Table.builder()
            .type(PROVIDER.getTableType())
            .name("testTable")
            .schema(schema)
            .location(location)
            .properties(new JSONObject().fluentPutAll(properties))
            .build();
    return PROVIDER.buildBeamSqlTable(table);
  }

  @Test
  public void invalidSchemas() {
    Function<Schema, BeamSqlTable> tableMaker =
        schema -> makeTable(schema, example(SubscriptionPath.class).toString(), ImmutableMap.of());
    // No payload
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder().addDateTimeField(RowHandler.EVENT_TIMESTAMP_FIELD).build()));
    // Bad payload type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(Schema.builder().addDateTimeField(RowHandler.PAYLOAD_FIELD).build()));
    // Bad field name
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder()
                    .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                    .addByteArrayField("my-random-field")
                    .build()));
    // Bad attributes type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder()
                    .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                    .addByteArrayField(RowHandler.ATTRIBUTES_FIELD)
                    .build()));
    // Bad attributes field names
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder()
                    .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                    .addRowField(
                        RowHandler.ATTRIBUTES_FIELD,
                        Schema.builder()
                            .addStringField(RowHandler.ATTRIBUTES_KEY_FIELD)
                            .addArrayField("badValues", FieldType.BYTES)
                            .build())
                    .build()));
    // Bad event timestamp type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder()
                    .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                    .addByteArrayField(RowHandler.EVENT_TIMESTAMP_FIELD)
                    .build()));
    // Bad publish timestamp type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder()
                    .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                    .addByteArrayField(RowHandler.PUBLISH_TIMESTAMP_FIELD)
                    .build()));
    // Bad message key type
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableMaker.apply(
                Schema.builder()
                    .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                    .addStringField(RowHandler.MESSAGE_KEY_FIELD)
                    .build()));
  }

  @Test
  public void bytesWithFormatInvalid() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            makeTable(
                Schema.builder().addByteArrayField(RowHandler.PAYLOAD_FIELD).build(),
                example(SubscriptionPath.class).toString(),
                ImmutableMap.of("format", "json")));
  }

  @Test
  public void invalidLocations() {
    Function<String, BeamSqlTable> tableMaker =
        location -> makeTable(FULL_WRITE_SCHEMA, location, ImmutableMap.of());

    // Just nonsense
    assertThrows(IllegalArgumentException.class, () -> tableMaker.apply("my-location"));
    // CPS topic
    assertThrows(ApiException.class, () -> tableMaker.apply("projects/abc/topics/def"));
    // CPS subscription
    assertThrows(ApiException.class, () -> tableMaker.apply("projects/abc/subscriptions/def"));
  }

  @Test
  public void validTopicTables() {
    BeamSqlTable basic =
        makeTable(FULL_WRITE_SCHEMA, example(TopicPath.class).toString(), ImmutableMap.of());
    assertTrue(basic instanceof PubsubLiteTopicTable);
    BeamSqlTable row =
        makeTable(
            Schema.builder()
                .addRowField(
                    RowHandler.PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                .build(),
            example(TopicPath.class).toString(),
            ImmutableMap.of("format", "json")); // Defaults to json
    assertTrue(row instanceof PubsubLiteTopicTable);
    BeamSqlTable dlq =
        makeTable(
            Schema.builder()
                .addRowField(
                    RowHandler.PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                .build(),
            example(TopicPath.class).toString(),
            ImmutableMap.of(
                "deadLetterQueue", "pubsub:projects/abc/topics/def")); // Defaults to json
    assertTrue(dlq instanceof PubsubLiteTopicTable);
  }

  @Test
  @SuppressWarnings("argument")
  public void topicTableCannotRead() {
    BeamSqlTable basic =
        makeTable(FULL_WRITE_SCHEMA, example(TopicPath.class).toString(), ImmutableMap.of());
    assertThrows(UnsupportedOperationException.class, () -> basic.buildIOReader(null));
  }

  @Test
  public void validSubscriptionTables() {
    BeamSqlTable basic =
        makeTable(FULL_READ_SCHEMA, example(SubscriptionPath.class).toString(), ImmutableMap.of());
    assertTrue(basic instanceof PubsubLiteSubscriptionTable);
    BeamSqlTable row =
        makeTable(
            Schema.builder()
                .addRowField(
                    RowHandler.PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                .build(),
            example(SubscriptionPath.class).toString(),
            ImmutableMap.of("format", "json"));
    assertTrue(row instanceof PubsubLiteSubscriptionTable);
    BeamSqlTable dlq =
        makeTable(
            Schema.builder()
                .addRowField(
                    RowHandler.PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                .build(),
            example(SubscriptionPath.class).toString(),
            ImmutableMap.of("format", "json", "deadLetterQueue", "pubsub:projects/abc/topics/def"));
    assertTrue(dlq instanceof PubsubLiteSubscriptionTable);
  }

  @Test
  @SuppressWarnings("argument")
  public void subscriptionTableCannotWrite() {
    BeamSqlTable basic =
        makeTable(FULL_READ_SCHEMA, example(SubscriptionPath.class).toString(), ImmutableMap.of());
    assertThrows(UnsupportedOperationException.class, () -> basic.buildIOWriter(null));
  }
}
