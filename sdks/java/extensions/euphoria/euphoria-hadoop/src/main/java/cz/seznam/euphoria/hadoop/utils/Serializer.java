/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Java serializer.
 */
public class Serializer {

  /**
   * Convert a {@code Serializable} to bytes using java serialization.
   *
   * @param object the object to serialize
   *
   * @return the serialized form of the given object; never {@code null}
   *
   * @throws IOException if serializing the given object fails for some reason
   */
  public static byte[] toBytes(Serializable object) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(object);
    oos.close();
    return baos.toByteArray();
  }

  /**
   * Convert bytes to {@code Serializable} object.
   *
   * @param <T> the type of the object to deserialized; if the actual object
   *             is not of this type, a {@link ClassCastException} is expected
   *             at the side of the calling code
   * @param bytes the java serialized form of the object to deserialize
   *
   * @return the deserialized object; never {@code null}
   *
   * @throws IOException if deserializing the given data bytes fails for some reason
   * @throws ClassNotFoundException if the actual object to be deserialized represents
   *          a type that is not known on the classpath
   */
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T fromBytes(byte[] bytes)
      throws IOException, ClassNotFoundException {
    
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    return (T) ois.readObject();
  }

}
