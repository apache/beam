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

  private static final String AVRO_SCHEMA_FILE = "avro_all_data_types_flat_schema.json";
  private static final String PROTO_PRIMITIVE_TYPES_FLAT = "proto-primitive-types-flat";

  private static final String PROTO_SCHEMA_FILE = "all_data_types_flat_schema.proto";

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
        hasAvroSchemaPath, AVRO_SCHEMA_FILE, com.google.pubsub.v1.Schema.Type.AVRO);
    pubsubClient.createSchema(
        hasProtoSchemaPath, PROTO_SCHEMA_FILE, com.google.pubsub.v1.Schema.Type.PROTOCOL_BUFFER);
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
}
