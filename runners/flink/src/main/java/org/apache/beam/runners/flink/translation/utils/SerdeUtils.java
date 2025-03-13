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
package org.apache.beam.runners.flink.translation.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** Util methods to help with serialization / deserialization. */
public class SerdeUtils {

  // Private constructor for a util class.
  private SerdeUtils() {}

  public static @Nonnull byte[] serializeObject(@Nullable Object obj) throws IOException {
    if (obj == null) {
      return new byte[0];
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return baos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  public static @Nullable Object deserializeObject(byte[] serialized) throws IOException {
    if (serialized == null || serialized.length == 0) {
      return null;
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  public static <T> SimpleVersionedSerializer<T> getNaiveObjectSerializer() {
    return new SimpleVersionedSerializer<T>() {
      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public byte[] serialize(T obj) throws IOException {
        return serializeObject(obj);
      }

      @Override
      @SuppressWarnings("unchecked")
      public T deserialize(int version, byte[] serialized) throws IOException {
        if (version > getVersion()) {
          throw new IOException(
              String.format(
                  "Received serialized object of version %d, which is higher than "
                      + "the highest supported version %d.",
                  version, getVersion()));
        }
        return (T) deserializeObject(serialized);
      }
    };
  }
}
