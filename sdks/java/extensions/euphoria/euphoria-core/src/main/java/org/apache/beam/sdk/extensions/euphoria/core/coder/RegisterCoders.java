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
package org.apache.beam.sdk.extensions.euphoria.core.coder;

import static java.util.Objects.requireNonNull;

import com.esotericsoftware.kryo.Kryo;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.stability.Experimental;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Convenient way of registering Beam {@link Coder}s to the given {@link Pipeline}. */
@Experimental
public class RegisterCoders {

  public static KryoBuilder to(Pipeline pipeline) {
    return new Builder(requireNonNull(pipeline));
  }

  // ----------------------------- builder chain

  /** Builder which allows to se {@link KryoRegistrar}. */
  public interface KryoBuilder extends RegisterBuilder {

    /**
     * Sets {@link KryoRegistrar}. All the classes registered by it are automatically coded using
     * {@link Kryo}.
     *
     * @param registrar user defined class registrations to {@link Kryo}
     * @return {@link RegisterBuilder} which allows user to register coders of its own
     */
    RegisterBuilder setKryoClassRegistrar(KryoRegistrar registrar);
  }

  /** Builder which defines all non {@link com.esotericsoftware.kryo.Kryo} registration methods. */
  public interface RegisterBuilder {

    /**
     * Registers custom {@link Coder} for given parametrized {@link TypeDescriptor}.
     *
     * @param type type to register coder for
     * @param coder coder to register
     * @param <T> type of elements encoded by given {@code coder}
     * @return {@link RegisterBuilder} to allow for more coders registration.
     */
    <T> RegisterBuilder registerCoder(TypeDescriptor<T> type, Coder<T> coder);

    /**
     * Registers custom {@link Coder} for given raw {@link Class type}.
     *
     * @param clazz type to register coder for
     * @param coder coder to register
     * @param <T> type of elements encoded by given {@code coder}
     * @return {@link RegisterBuilder} to allow for more coders registration.
     */
    <T> RegisterBuilder registerCoder(Class<T> clazz, Coder<T> coder);

    /** Effectively ends coders registration. No coders registration is done without it. */
    void done();
  }

  // ----------------------------- builder itself

  /** Builder of {@link RegisterCoders}. */
  public static class Builder implements RegisterBuilder, KryoBuilder {

    private final Pipeline pipeline;
    private final Map<TypeDescriptor, Coder<?>> typeToCoder = new HashMap<>();
    private final Map<Class<?>, Coder<?>> classToCoder = new HashMap<>();
    private KryoRegistrar registrar;

    Builder(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    @Override
    public <T> RegisterBuilder registerCoder(TypeDescriptor<T> type, Coder<T> coder) {
      requireNonNull(type);
      requireNonNull(coder);
      typeToCoder.put(type, coder);
      return this;
    }

    @Override
    public <T> RegisterBuilder registerCoder(Class<T> clazz, Coder<T> coder) {
      requireNonNull(clazz);
      requireNonNull(coder);
      classToCoder.put(clazz, coder);
      return this;
    }

    @Override
    public RegisterBuilder setKryoClassRegistrar(KryoRegistrar registrar) {
      requireNonNull(registrar);
      this.registrar = registrar;
      return this;
    }

    @Override
    public void done() {
      pipeline
          .getCoderRegistry()
          .registerCoderProvider(new EuphoriaCoderProvider(typeToCoder, classToCoder, registrar));
    }
  }
}
