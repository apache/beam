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
package org.apache.beam.sdk.io.thrift;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

public class ThriftCoder<T> extends CustomCoder<T> {

  public static <T> ThriftCoder<T> of() {
    return new ThriftCoder<>();
  }

  /**
   * Encodes the given value of type {@code T} onto the given output stream.
   *
   * @param value {@link org.apache.thrift.TBase} to encode.
   * @param outStream stream to output encoded value to.
   * @throws IOException if writing to the {@code OutputStream} fails for some reason
   * @throws CoderException if the value could not be encoded for some reason
   */
  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outStream);
    oos.writeObject(value);
    oos.flush();
  }

  /**
   * Decodes a value of type {@code T} from the given input stream in the given context. Returns the
   * decoded value.
   *
   * @param inStream
   * @throws IOException if reading from the {@code InputStream} fails for some reason
   * @throws CoderException if the value could not be decoded for some reason
   */
  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    try {

      ObjectInputStream ois = new ObjectInputStream(inStream);
      return (T) ois.readObject();
    } catch (Exception classNotFoundException) {
      throw new RuntimeException(
          "Could not deserialize bytes to Document" + classNotFoundException);
    }
  }
}
