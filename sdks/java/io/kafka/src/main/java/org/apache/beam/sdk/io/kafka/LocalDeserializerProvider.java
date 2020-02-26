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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A {@link DeserializerProvider} that given a local {@link Deserializer} class and Beam's {@link
 * CoderRegistry} configures a {@link Deserializer} instance and infers its corresponding {@link
 * Coder}.
 */
class LocalDeserializerProvider<T> implements DeserializerProvider<T> {
  private Class<? extends Deserializer<T>> deserializer;

  private LocalDeserializerProvider(Class<? extends Deserializer<T>> deserializer) {
    checkArgument(deserializer != null, "You should provide a deserializer.");
    this.deserializer = deserializer;
  }

  static <T> LocalDeserializerProvider<T> of(Class<? extends Deserializer<T>> deserializer) {
    return new LocalDeserializerProvider<>(deserializer);
  }

  @Override
  public Deserializer<T> getDeserializer(Map<String, ?> configs, boolean isKey) {
    try {
      Deserializer<T> deserializer = this.deserializer.getDeclaredConstructor().newInstance();
      deserializer.configure(configs, isKey);
      return deserializer;
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new RuntimeException("Could not instantiate deserializers", e);
    }
  }

  /**
   * Attempt to infer a {@link Coder} by extracting the type of the deserialized-class from the
   * deserializer argument using the {@link Coder} registry.
   */
  @Override
  public NullableCoder<T> getCoder(CoderRegistry coderRegistry) {
    for (Type type : deserializer.getGenericInterfaces()) {
      if (!(type instanceof ParameterizedType)) {
        continue;
      }

      // This does not recurse: we will not infer from a class that extends
      // a class that extends Deserializer<T>.
      ParameterizedType parameterizedType = (ParameterizedType) type;

      if (parameterizedType.getRawType() == Deserializer.class) {
        Type parameter = parameterizedType.getActualTypeArguments()[0];

        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) parameter;

        try {
          return NullableCoder.of(coderRegistry.getCoder(clazz));
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException(
              String.format(
                  "Unable to automatically infer a Coder for "
                      + "the Kafka Deserializer %s: no coder registered for type %s",
                  deserializer, clazz));
        }
      }
    }
    throw new RuntimeException(
        String.format("Could not extract the Kafka Deserializer type from %s", deserializer));
  }
}
