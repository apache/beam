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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import org.apache.avro.Schema;

/** Mock implementation of {@link CSRClientProvider} used for testing. */
class TestCSRClientProvider extends BasicCSRClientProvider {

  private static final String SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  public TestCSRClientProvider(
      String schemaRegistryUrl, String keySchemaSubject, String valueSchemaSubject) {
    super(schemaRegistryUrl, keySchemaSubject, valueSchemaSubject);
  }

  @Override
  public SchemaRegistryClient getCSRClient() {
    SchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    registerSchemaBySubject(registryClient, getKeySchemaSubject(), SCHEMA);
    registerSchemaBySubject(registryClient, getValueSchemaSubject(), SCHEMA);
    return registryClient;
  }

  private void registerSchemaBySubject(
      SchemaRegistryClient registryClient, String schemaSubject, Schema schema) {
    try {
      registryClient.register(schemaSubject, schema);
    } catch (IOException | RestClientException e) {
      throw new IllegalArgumentException(
          "Unable to register schema for subject: " + schemaSubject, e);
    }
  }
}
