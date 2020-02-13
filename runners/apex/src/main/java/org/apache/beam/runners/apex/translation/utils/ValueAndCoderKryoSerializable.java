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
package org.apache.beam.runners.apex.translation.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;

/**
 * A {@link KryoSerializable} holder that uses the specified {@link Coder}.
 *
 * @param <T> element type
 */
public class ValueAndCoderKryoSerializable<T> implements KryoSerializable {
  private static final JavaSerializer JAVA_SERIALIZER = new JavaSerializer();
  private T value;
  private Coder<T> coder;

  public ValueAndCoderKryoSerializable(T value, Coder<T> coder) {
    this.value = value;
    this.coder = coder;
  }

  @SuppressWarnings("unused") // for Kryo
  private ValueAndCoderKryoSerializable() {}

  public T get() {
    return value;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    try {
      kryo.writeClass(output, coder.getClass());
      kryo.writeObject(output, coder, JAVA_SERIALIZER);
      coder.encode(value, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    try {
      @SuppressWarnings("unchecked")
      Class<Coder<T>> type = kryo.readClass(input).getType();
      coder = kryo.readObject(input, type, JAVA_SERIALIZER);
      value = coder.decode(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
