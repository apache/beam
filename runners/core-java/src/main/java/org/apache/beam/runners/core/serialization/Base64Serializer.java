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
package org.apache.beam.runners.core.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

/** A utility class for serializing and deserializing Java objects into a Base64 string. */
public class Base64Serializer {
  private Base64Serializer() {}

  public static String serializeUnchecked(Serializable serializable) {
    try {
      return serialize(serializable);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T deserializeUnchecked(String serialized, Class<T> klass) {
    try {
      return deserialize(serialized, klass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T deserialize(String serialized, Class<T> klass)
      throws IOException, ClassNotFoundException {
    final byte[] bytes = Base64.getDecoder().decode(serialized);
    final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
    @SuppressWarnings("unchecked")
    T object = (T) ois.readObject();
    ois.close();
    return object;
  }

  private static String serialize(Serializable serializable) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(serializable);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }
}
