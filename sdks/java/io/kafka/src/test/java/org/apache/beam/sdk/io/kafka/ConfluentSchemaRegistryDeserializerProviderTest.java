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
package org.apache.beam.sdk.io.kafka;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.IOException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConfluentSchemaRegistryDeserializerProviderTest {
  @Test
  public void testGetCoder() {
    String schemaRegistryUrl = "mock://my-scope-name";
    String subject = "mytopic";
    SchemaRegistryClient mockRegistryClient = mockSchemaRegistryClient(schemaRegistryUrl, subject);
    CoderRegistry coderRegistry = CoderRegistry.createDefault();

    AvroCoder coderV0 =
        (AvroCoder)
            mockDeserializerProvider(schemaRegistryUrl, subject, null).getCoder(coderRegistry);
    assertEquals(AVRO_SCHEMA, coderV0.getSchema());

    try {
      Integer version = mockRegistryClient.register(subject, AVRO_SCHEMA_V1);
      AvroCoder coderV1 =
          (AvroCoder)
              mockDeserializerProvider(schemaRegistryUrl, subject, version).getCoder(coderRegistry);
      assertEquals(AVRO_SCHEMA_V1, coderV1.getSchema());
    } catch (IOException | RestClientException e) {
      throw new RuntimeException("Unable to register schema for subject: " + subject, e);
    }
  }

  static <T> DeserializerProvider<T> mockDeserializerProvider(
      String schemaRegistryUrl, String subject, Integer version) {
    return new ConfluentSchemaRegistryDeserializerProvider<>(
        (SerializableFunction<Void, SchemaRegistryClient>)
            input -> mockSchemaRegistryClient(schemaRegistryUrl, subject),
        schemaRegistryUrl,
        subject,
        version);
  }

  private static SchemaRegistryClient mockSchemaRegistryClient(
      String schemaRegistryUrl, String subject) {
    SchemaRegistryClient mockRegistryClient =
        MockSchemaRegistry.getClientForScope(schemaRegistryUrl);
    try {
      mockRegistryClient.register(subject, AVRO_SCHEMA);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException("Unable to register schema for subject: " + subject, e);
    }
    return mockRegistryClient;
  }

  private static final String AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static final org.apache.avro.Schema AVRO_SCHEMA =
      new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_STRING);

  private static final String AVRO_SCHEMA_V1_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"age\", \"type\": \"int\"},\n"
          + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";
  private static final org.apache.avro.Schema AVRO_SCHEMA_V1 =
      new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_V1_STRING);
}
