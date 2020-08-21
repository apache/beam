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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link DeserializerProvider} that uses <a
 * href="https://github.com/confluentinc/schema-registry">Confluent Schema Registry</a> to resolve a
 * {@link Deserializer}s and {@link Coder} given a subject.
 */
@Experimental(Kind.SOURCE_SINK)
public class ConfluentSchemaRegistryDeserializerProvider<T> implements DeserializerProvider<T> {
  private final SerializableFunction<Void, SchemaRegistryClient> schemaRegistryClientProviderFn;
  private final String schemaRegistryUrl;
  private final String subject;
  private final @Nullable Integer version;

  @VisibleForTesting
  ConfluentSchemaRegistryDeserializerProvider(
      SerializableFunction<Void, SchemaRegistryClient> schemaRegistryClientProviderFn,
      String schemaRegistryUrl,
      String subject,
      @Nullable Integer version) {
    checkArgument(
        schemaRegistryClientProviderFn != null,
        "You should provide a schemaRegistryClientProviderFn.");
    checkArgument(schemaRegistryUrl != null, "You should provide a schemaRegistryUrl.");
    checkArgument(subject != null, "You should provide a subject to fetch the schema from.");
    this.schemaRegistryClientProviderFn = schemaRegistryClientProviderFn;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.subject = subject;
    this.version = version;
  }

  public static <T> ConfluentSchemaRegistryDeserializerProvider<T> of(
      String schemaRegistryUrl, String subject) {
    return of(schemaRegistryUrl, subject, null);
  }

  public static <T> ConfluentSchemaRegistryDeserializerProvider<T> of(
      String schemaRegistryUrl, String subject, @Nullable Integer version) {
    return new ConfluentSchemaRegistryDeserializerProvider(
        (SerializableFunction<Void, SchemaRegistryClient>)
            input -> new CachedSchemaRegistryClient(schemaRegistryUrl, Integer.MAX_VALUE),
        schemaRegistryUrl,
        subject,
        version);
  }

  @Override
  public Deserializer<T> getDeserializer(Map<String, ?> configs, boolean isKey) {
    ImmutableMap<String, Object> csrConfig =
        ImmutableMap.<String, Object>builder()
            .putAll(configs)
            .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
            .build();
    Deserializer<T> deserializer =
        (Deserializer<T>)
            new ConfluentSchemaRegistryDeserializer(
                getSchemaRegistryClient(),
                new Schema.Parser().parse(getSchemaMetadata().getSchema()));
    deserializer.configure(csrConfig, isKey);
    return deserializer;
  }

  @Override
  public Coder<T> getCoder(CoderRegistry coderRegistry) {
    final Schema avroSchema = new Schema.Parser().parse(getSchemaMetadata().getSchema());
    return (Coder<T>) AvroCoder.of(avroSchema);
  }

  private SchemaMetadata getSchemaMetadata() {
    try {
      return (version == null)
          ? getSchemaRegistryClient().getLatestSchemaMetadata(subject)
          : getSchemaRegistryClient().getSchemaMetadata(subject, version);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException("Unable to get latest schema metadata for subject: " + subject, e);
    }
  }

  private SchemaRegistryClient getSchemaRegistryClient() {
    return this.schemaRegistryClientProviderFn.apply(null);
  }
}

class ConfluentSchemaRegistryDeserializer extends KafkaAvroDeserializer {
  Schema readerSchema;

  ConfluentSchemaRegistryDeserializer(SchemaRegistryClient client, Schema readerSchema) {
    super(client);
    this.readerSchema = readerSchema;
  }

  @Override
  public Object deserialize(String s, byte[] bytes) {
    return this.deserialize(bytes, readerSchema);
  }
}
