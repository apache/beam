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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.stability.Experimental;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BeamFlow;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Convenient way of registering Beam {@link Coder} to given {@link Pipeline} or {@link BeamFlow}.
 */
@Experimental
public class RegisterCoders {

  public static KryoBuilder to(Flow flow) {
    Objects.requireNonNull(flow);

    if (flow instanceof BeamFlow) {
      BeamFlow beamFlow = (BeamFlow) flow;
      return new Builder(beamFlow);
    }

    throw new IllegalArgumentException(
        "Given flow is not an instance of " + BeamFlow.class.getName() + " .");
  }

  public static KryoBuilder to(BeamFlow flow) {
    return new Builder(Objects.requireNonNull(flow));
  }

  // ----------------------------- builder chain

  /** Builder which allows to se {@link KryoRegistrar}. */
  public interface KryoBuilder extends RegisterBuilder {

    /**
     * Sets {@link KryoRegistrar}. All the classes registered by it are automatically coded using
     * {@link Kryo}.
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

    private final BeamFlow beamFlow;
    private final Map<TypeDescriptor, Coder<?>> typeToCoder = new HashMap<>();
    private final Map<Class<?>, Coder<?>> classToCoder = new HashMap<>();
    private KryoRegistrar registrar;

    Builder(BeamFlow beamFlow) {
      this.beamFlow = beamFlow;
    }

    @Override
    public <T> RegisterBuilder registerCoder(TypeDescriptor<T> type, Coder<T> coder) {
      Objects.requireNonNull(type);
      Objects.requireNonNull(coder);
      typeToCoder.put(type, coder);
      return this;
    }

    @Override
    public <T> RegisterBuilder registerCoder(Class<T> clazz, Coder<T> coder) {
      Objects.requireNonNull(clazz);
      Objects.requireNonNull(coder);
      classToCoder.put(clazz, coder);
      return this;
    }

    @Override
    public RegisterBuilder setKryoClassRegistrar(KryoRegistrar registrar) {
      Objects.requireNonNull(registrar);
      this.registrar = registrar;
      return this;
    }

    @Override
    public void done() {
      EuphoriaCoderProvider provider =
          new EuphoriaCoderProvider(typeToCoder, classToCoder, registrar);

      beamFlow.setEuphoriaCoderProvider(provider);
    }
  }
}
