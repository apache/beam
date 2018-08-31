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
package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.OutputChunked;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

/**
 * Coder using Kryo as (de)serialization mechanism. See {@link RegisterCoders} to get more details
 * of how to use it
 */
public class KryoCoder<T> extends CustomCoder<T> {

  /**
   * Client-defined class registrations to {@link Kryo}.
   *
   * <p>{@link KryoCoder} needs it to be able to create a {@link Kryo} instance with correct class
   * registrations after its deserialization.
   */
  private final IdentifiedRegistrar registrarWithId;

  private KryoCoder(IdentifiedRegistrar registrarWithId) {
    this.registrarWithId = registrarWithId;
  }

  /**
   * Creates a {@link KryoCoder} instance which will use {@link Kryo} with classes registered by
   * {@code registrar}.
   */
  public static <T> KryoCoder<T> of(IdentifiedRegistrar registrarWithId) {
    return new KryoCoder<>(registrarWithId);
  }

  /**
   * Creates a {@link KryoCoder} instance which will use {@link Kryo} without class registration.
   * That degrades performance. Use {@link #of(IdentifiedRegistrar)} whenever possible.
   */
  public static <T> KryoCoder<T> withoutClassRegistration() {
    return new KryoCoder<>(KryoFactory.NO_OP_REGISTRAR);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {

    Kryo kryo = KryoFactory.getOrCreateKryo(registrarWithId);

    OutputChunked output = KryoFactory.getKryoOutput();
    output.clear();
    output.setOutputStream(outStream);

    try {
      kryo.writeClassAndObject(output, value);
      output.endChunks();
      output.flush();
    } catch (IllegalArgumentException e) {
      throw new CoderException(
          String.format(
              "Cannot encode given object of type '%s'. "
                  + "Forgotten kryo registration is possible explanation. Kryo registrations where done by '%s'.",
              (value == null) ? null : value.getClass().getSimpleName(), registrarWithId),
          e);
    }
  }

  @Override
  public T decode(InputStream inStream) throws IOException {

    InputChunked input = KryoFactory.getKryoInput();
    input.rewind();
    input.setInputStream(inStream);

    Kryo kryo = KryoFactory.getOrCreateKryo(registrarWithId);

    try {
      @SuppressWarnings("unchecked")
      T outObject = (T) kryo.readClassAndObject(input);
      return outObject;

    } catch (KryoException e) {
      throw new CoderException(
          String.format(
              "Cannot decode object from input stream."
                  + " Forgotten kryo registration is possible explanation. Kryo registrations where done by '%s'.",
              registrarWithId),
          e);
    }
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // nop
  }
}
