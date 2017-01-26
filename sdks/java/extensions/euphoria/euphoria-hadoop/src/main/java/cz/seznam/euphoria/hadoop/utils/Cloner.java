/**
 * Copyright 2016 Seznam a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.hadoop.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * External cloner for type.
 */
public interface Cloner<T> {

  /** Clone given instance. */
  T clone(T what);

  /** Get cloner for given class type. */
  public static <T> Cloner<T> get(Class<T> what, Configuration conf) {
    SerializationFactory factory = new SerializationFactory(conf);
    Serialization<T> serialization = factory.getSerialization(what);
    if (serialization == null) {
      // FIXME: if we cannot (de)serialize just do not clone
      return t -> t;
    }
    Deserializer<T> deserializer = serialization.getDeserializer(what);
    Serializer<T> serializer = serialization.getSerializer(what);

    return (T elem) -> {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.open(baos);
        serializer.serialize(elem);
        serializer.close();
        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        deserializer.open(bais);
        T deserialized = deserializer.deserialize(null);
        deserializer.close();
        return deserialized;
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };
  }
}
