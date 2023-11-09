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
package org.apache.beam.io.requestresponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.io.IOException;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * An implementation of {@link CacheSerializerProvider} that provides a {@link CacheSerializer}
 * responsible for converting between a user defined type to a byte array representation of a JSON
 * string.
 */
@AutoService(CacheSerializerProvider.class)
public class JsonCacheSerializerProvider implements CacheSerializerProvider {
  private static final String IDENTIFIER = "json";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public Schema getConfigurationSchema() {
    return null;
  }

  @Override
  public <T> CacheSerializer<T> getSerializer(Class<T> clazz) {
    if (!OBJECT_MAPPER.canSerialize(clazz)) {
      throw new IllegalArgumentException(clazz + " cannot serialize to JSON");
    }
    return new JsonCacheSerializer<>(new ToJsonFn<>(), new FromJsonFn<>(clazz));
  }

  private static class JsonCacheSerializer<T> implements CacheSerializer<T> {

    private final ToJsonFn<T> serializeFn;
    private final FromJsonFn<T> deserializeFn;

    private JsonCacheSerializer(ToJsonFn<T> serializeFn, FromJsonFn<T> deserializeFn) {
      this.serializeFn = serializeFn;
      this.deserializeFn = deserializeFn;
    }

    @Override
    public T deserialize(byte[] bytes) throws UserCodeExecutionException {
      return deserializeFn.apply(bytes);
    }

    @Override
    public byte[] serialize(T t) throws UserCodeExecutionException {
      return serializeFn.apply(t);
    }
  }

  private static class ToJsonFn<T> implements ProcessFunction<@NonNull T, byte[]> {

    @Override
    public byte[] apply(@NonNull T input) throws UserCodeExecutionException {
      try {
        return OBJECT_MAPPER.writeValueAsBytes(input);
      } catch (JsonProcessingException e) {
        throw new UserCodeExecutionException(e);
      }
    }
  }

  private static class FromJsonFn<T> implements ProcessFunction<byte[], @NonNull T> {

    private final Class<T> clazz;

    private FromJsonFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public @NonNull T apply(byte[] input) throws UserCodeExecutionException {
      try {
        return OBJECT_MAPPER.readValue(input, clazz);
      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
    }
  }
}
