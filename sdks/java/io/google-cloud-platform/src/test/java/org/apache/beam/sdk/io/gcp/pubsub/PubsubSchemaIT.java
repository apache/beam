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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SchemaPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link Schema} related {@link PubsubClient} operations. */
@RunWith(JUnit4.class)
public class PubsubSchemaIT {
  private static final String HAS_NO_SCHEMA = "has-no-schema";
  private static final String HAS_AVRO_SCHEMA = "has-avro-schema";
  private static final String HAS_PROTO_SCHEMA = "has-proto-schema";
  private static final String AVRO_PRIMITIVE_TYPES_FLAT = "avro-primitive-types-flat";

  private static final String PROTO_PRIMITIVE_TYPES_FLAT = "proto-primitive-types-flat";

  private static PubsubClient pubsubClient;

  private static TopicPath hasNoSchemaTopic;

  private static TopicPath hasAvroSchemaTopic;

  private static TopicPath hasProtoSchemaTopic;

  private static SchemaPath hasAvroSchemaPath;

  private static SchemaPath hasProtoSchemaPath;

  static final Schema ALL_DATA_TYPES_AVRO_SCHEMA =
      Schema.of(
          Field.of("BooleanField", FieldType.BOOLEAN),
          Field.of("IntField", FieldType.INT32),
          Field.of("LongField", FieldType.INT64),
          Field.of("FloatField", FieldType.FLOAT),
          Field.of("DoubleField", FieldType.DOUBLE),
          Field.of("StringField", FieldType.STRING));

  @BeforeClass
  public static void setup() throws IOException {
    PubsubOptions options = TestPipeline.testingPipelineOptions().as(PubsubOptions.class);
    String project = options.getProject();
    String postFix = "-" + Instant.now().getMillis();
    pubsubClient = PubsubGrpcClient.FACTORY.newClient(null, null, options);
    hasNoSchemaTopic = PubsubClient.topicPathFromName(project, HAS_NO_SCHEMA + postFix);
    hasAvroSchemaTopic = PubsubClient.topicPathFromName(project, HAS_AVRO_SCHEMA + postFix);
    hasProtoSchemaTopic = PubsubClient.topicPathFromName(project, HAS_PROTO_SCHEMA + postFix);
    hasAvroSchemaPath = PubsubClient.schemaPathFromId(project, AVRO_PRIMITIVE_TYPES_FLAT + postFix);
    hasProtoSchemaPath =
        PubsubClient.schemaPathFromId(project, PROTO_PRIMITIVE_TYPES_FLAT + postFix);

    pubsubClient.createSchema(
        hasAvroSchemaPath, AVRO_ALL_DATA_TYPES_FLAT_SCHEMA, com.google.pubsub.v1.Schema.Type.AVRO);
    pubsubClient.createSchema(
        hasProtoSchemaPath,
        PROTO_ALL_DATA_TYPES_FLAT_SCHEMA,
        com.google.pubsub.v1.Schema.Type.PROTOCOL_BUFFER);
    pubsubClient.createTopic(hasNoSchemaTopic);
    pubsubClient.createTopic(hasAvroSchemaTopic, hasAvroSchemaPath);
    pubsubClient.createTopic(hasProtoSchemaTopic, hasProtoSchemaPath);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    pubsubClient.deleteTopic(hasNoSchemaTopic);
    pubsubClient.deleteTopic(hasAvroSchemaTopic);
    pubsubClient.deleteTopic(hasProtoSchemaTopic);
    pubsubClient.deleteSchema(hasAvroSchemaPath);
    pubsubClient.deleteSchema(hasProtoSchemaPath);
    pubsubClient.close();
  }

  @Test
  public void testGetSchemaPath() throws IOException {
    assertNull(pubsubClient.getSchemaPath(hasNoSchemaTopic));

    assertEquals(
        hasAvroSchemaPath.getPath(), pubsubClient.getSchemaPath(hasAvroSchemaTopic).getPath());

    assertEquals(
        hasProtoSchemaPath.getPath(), pubsubClient.getSchemaPath(hasProtoSchemaTopic).getPath());
  }

  @Test
  public void testGetSchema() throws IOException {
    assertEquals(ALL_DATA_TYPES_AVRO_SCHEMA, pubsubClient.getSchema(hasAvroSchemaPath));

    assertThrows(
        "Pub/Sub schema type PROTOCOL_BUFFER is not supported at this time",
        IllegalArgumentException.class,
        () -> pubsubClient.getSchema(hasProtoSchemaPath));
  }

  private static final String PROTO_ALL_DATA_TYPES_FLAT_SCHEMA =
      "syntax = \"proto3\";\n"
          + "\n"
          + "message Record {\n"
          + "  double doubleField = 1;\n"
          + "  float floatField = 2;\n"
          + "  int32 int32Field = 3;\n"
          + "  int64 int64Field = 4;\n"
          + "  bool boolField = 5;\n"
          + "  string stringField = 6;\n"
          + "}";

  private static final String AVRO_ALL_DATA_TYPES_FLAT_SCHEMA =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"Avro\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"BooleanField\",\n"
          + "      \"type\": \"boolean\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"IntField\",\n"
          + "      \"type\": \"int\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"LongField\",\n"
          + "      \"type\": \"long\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"FloatField\",\n"
          + "      \"type\": \"float\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"DoubleField\",\n"
          + "      \"type\": \"double\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"StringField\",\n"
          + "      \"type\": \"string\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";
}
