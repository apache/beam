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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import javax.annotation.Nullable;

/** Basic implementation of {@link CSRClientProvider} used by default in {@link KafkaIO}. */
class BasicCSRClientProvider implements CSRClientProvider {
  @Nullable private final String keySchemaSubject;
  @Nullable private final String valueSchemaSubject;
  private final String schemaRegistryUrl;

  static BasicCSRClientProvider of(
      String schemaRegistryUrl, String keySchemaSubject, String valueSchemaSubject) {
    return new BasicCSRClientProvider(schemaRegistryUrl, keySchemaSubject, valueSchemaSubject);
  }

  BasicCSRClientProvider(
      String schemaRegistryUrl, String keySchemaSubject, String valueSchemaSubject) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.keySchemaSubject = keySchemaSubject;
    this.valueSchemaSubject = valueSchemaSubject;
  }

  @Override
  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  @Override
  public String getKeySchemaSubject() {
    return keySchemaSubject;
  }

  @Override
  public String getValueSchemaSubject() {
    return valueSchemaSubject;
  }

  @Override
  public SchemaRegistryClient getCSRClient() {
    return new CachedSchemaRegistryClient(schemaRegistryUrl, Integer.MAX_VALUE);
  }
}
