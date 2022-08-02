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
package org.apache.beam.sdk.extensions.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.OutputChunked;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Coder using Kryo as (de)serialization mechanism. See {@link KryoCoderProvider} to get more
 * details about usage.
 *
 * @param <T> type of element coder can handle
 */
@Experimental
public class KryoCoder<T> extends CustomCoder<T> {

  /**
   * Create a new {@link KryoCoder} with default {@link KryoOptions}.
   *
   * @param <T> type of element this class should decode/encode {@link Kryo} instance used by
   *     returned {@link KryoCoder}
   * @return Newly created a {@link KryoCoder}
   */
  public static <T> KryoCoder<T> of() {
    return of(PipelineOptionsFactory.create(), Collections.emptyList());
  }

  /**
   * Create a new {@link KryoCoder} with default {@link KryoOptions}.
   *
   * @param registrars {@link KryoRegistrar}s which are used to register classes with underlying
   *     kryo instance
   * @param <T> type of element this class should decode/encode {@link Kryo} instance used by
   *     returned {@link KryoCoder}
   * @return Newly created a {@link KryoCoder}
   */
  public static <T> KryoCoder<T> of(KryoRegistrar... registrars) {
    return of(PipelineOptionsFactory.create(), registrars);
  }

  /**
   * Create a new {@link KryoCoder} with default {@link KryoOptions}.
   *
   * @param registrars {@link KryoRegistrar}s which are used to register classes with underlying
   *     kryo instance
   * @param <T> type of element this class should decode/encode {@link Kryo} instance used by
   *     returned {@link KryoCoder}
   * @return Newly created a {@link KryoCoder}
   */
  public static <T> KryoCoder<T> of(List<KryoRegistrar> registrars) {
    return of(PipelineOptionsFactory.create(), registrars);
  }

  /**
   * Create a new {@link KryoCoder}.
   *
   * @param pipelineOptions Options used for coder setup. See {@link KryoOptions} for more details.
   * @param <T> type of element this class should decode/encode {@link Kryo} instance used by
   *     returned {@link KryoCoder}
   * @return Newly created a {@link KryoCoder}
   */
  public static <T> KryoCoder<T> of(PipelineOptions pipelineOptions) {
    return of(pipelineOptions, Collections.emptyList());
  }

  /**
   * Create a new {@link KryoCoder}.
   *
   * @param pipelineOptions Options used for coder setup. See {@link KryoOptions} for more details.
   * @param registrars {@link KryoRegistrar}s which are used to register classes with underlying
   *     kryo instance
   * @param <T> type of element this class should decode/encode {@link Kryo} instance used by
   *     returned {@link KryoCoder}
   * @return Newly created a {@link KryoCoder}
   */
  public static <T> KryoCoder<T> of(PipelineOptions pipelineOptions, KryoRegistrar... registrars) {
    return of(pipelineOptions, Arrays.asList(registrars));
  }

  /**
   * Create a new {@link KryoCoder}.
   *
   * @param pipelineOptions Options used for coder setup. See {@link KryoOptions} for more details.
   * @param registrars {@link KryoRegistrar}s which are used to register classes with underlying
   *     kryo instance
   * @param <T> type of element this class should decode/encode {@link Kryo} instance used by
   *     returned {@link KryoCoder}
   * @return Newly created a {@link KryoCoder}
   */
  public static <T> KryoCoder<T> of(
      PipelineOptions pipelineOptions, List<KryoRegistrar> registrars) {
    final KryoOptions kryoOptions = pipelineOptions.as(KryoOptions.class);
    return new KryoCoder<>(
        new SerializableOptions(
            kryoOptions.getKryoBufferSize(),
            kryoOptions.getKryoReferences(),
            kryoOptions.getKryoRegistrationRequired()),
        registrars);
  }

  /** Serializable wrapper for {@link KryoOptions}. */
  static class SerializableOptions implements Serializable {

    /** Size of input and output buffer. */
    private final int bufferSize;

    /** Enables kryo reference tracking. */
    private final boolean references;

    /** Enables kryo required registration. */
    private final boolean registrationRequired;

    private SerializableOptions(int bufferSize, boolean references, boolean registrationRequired) {
      this.bufferSize = bufferSize;
      this.references = references;
      this.registrationRequired = registrationRequired;
    }

    /**
     * {@link SerializableOptions#bufferSize}.
     *
     * @return buffer size
     */
    int getBufferSize() {
      return bufferSize;
    }

    /**
     * {@link SerializableOptions#references}.
     *
     * @return boolean flag
     */
    boolean getReferences() {
      return references;
    }

    /**
     * {@link SerializableOptions#registrationRequired}.
     *
     * @return boolean flag
     */
    boolean getRegistrationRequired() {
      return registrationRequired;
    }
  }

  /** Unique id of the {@link KryoCoder} instance. */
  private final String instanceId = UUID.randomUUID().toString();

  /** Options for underlying kryo instance. */
  private final SerializableOptions options;

  /** Client-defined class registrations to {@link Kryo}. */
  private final List<KryoRegistrar> registrars;

  private KryoCoder(SerializableOptions options, List<KryoRegistrar> registrars) {
    this.options = options;
    this.registrars = registrars;
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    final KryoState kryoState = KryoState.get(this);
    if (value == null) {
      throw new CoderException("Cannot encode a null value.");
    }
    final OutputChunked outputChunked = kryoState.getOutputChunked();
    outputChunked.setOutputStream(outStream);
    try {
      kryoState.getKryo().writeClassAndObject(outputChunked, value);
      outputChunked.endChunks();
      outputChunked.flush();
    } catch (KryoException e) {
      outputChunked.clear();
      if (e.getCause() instanceof EOFException) {
        throw (EOFException) e.getCause();
      }
      throw new CoderException("Cannot encode given object of type [" + value.getClass() + "].", e);
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      if (message != null) {
        if (message.startsWith("Class is not registered")) {
          throw new CoderException(message);
        }
      }
      throw e;
    }
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    final KryoState kryoState = KryoState.get(this);
    final InputChunked inputChunked = kryoState.getInputChunked();
    inputChunked.setInputStream(inStream);
    try {
      @SuppressWarnings("unchecked")
      final T instance = (T) kryoState.getKryo().readClassAndObject(inputChunked);
      return instance;
    } catch (KryoException e) {
      throw new CoderException("Cannot decode object from input stream.", e);
    }
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // noop
  }

  /**
   * Create a new {@link KryoCoder} instance with the user provided registrar.
   *
   * @param registrar registrar to append to list of already registered registrars.
   * @return new kryo coder
   */
  public KryoCoder<T> withRegistrar(KryoRegistrar registrar) {
    final List<KryoRegistrar> newRegistrars = new ArrayList<>(registrars);
    registrars.add(registrar);
    return new KryoCoder<>(options, newRegistrars);
  }

  /**
   * {@link KryoCoder#instanceId}.
   *
   * @return instance id
   */
  String getInstanceId() {
    return instanceId;
  }

  /**
   * {@link KryoCoder#options}.
   *
   * @return options
   */
  SerializableOptions getOptions() {
    return options;
  }

  /**
   * {@link KryoCoder#registrars}.
   *
   * @return registrars
   */
  List<KryoRegistrar> getRegistrars() {
    return registrars;
  }

  @Override
  public int hashCode() {
    return instanceId.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other != null && getClass().equals(other.getClass())) {
      return instanceId.equals(((KryoCoder) other).instanceId);
    }
    return false;
  }
}
