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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * A source of {@link Kryo} instances. It allows {@link Kryo} to be reused by many {@link KryoCoder
 * KryoCoders}.
 */
class KryoFactory {

  /**
   * Initial size of byte buffers in {@link Output}, {@link Input}.
   */
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  /**
   * No-op {@link IdentifiedRegistrar}. Use of this registrar degrades performance since {@link
   * Kryo} needs to serialize fully specified class name instead of id.
   * <p>
   * {@link #getOrCreateKryo(IdentifiedRegistrar)} returns {@link Kryo}  which allows for
   * serialization of unregistered classes when this {@link IdentifiedRegistrar} is used to call
   * it.
   * </p>
   */
  static final IdentifiedRegistrar NO_OP_REGISTRAR = IdentifiedRegistrar.of((k) -> {
  });

  private static Kryo createKryo(IdentifiedRegistrar registrarWithId) {
    final Kryo instance = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) instance.getInstantiatorStrategy())
        .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    // mere NO_OP_REGISTRAR == registrarWithId is not enough since
    // NO_OP_REGISTRAR can be deserialized into several instances
    if (NO_OP_REGISTRAR.getId() == registrarWithId.getId()) {
      instance.setRegistrationRequired(false);
    } else {
      instance.setRegistrationRequired(true);
      registrarWithId.getRegistrar().registerClasses(instance);
    }

    return instance;
  }

  private static ThreadLocal<Output> threadLocalOutput = ThreadLocal.withInitial(
      () -> new Output(DEFAULT_BUFFER_SIZE, -1)
  );

  private static ThreadLocal<Input> threadLocalInput = ThreadLocal.withInitial(
      () -> new Input(DEFAULT_BUFFER_SIZE)
  );

  /**
   * We need an instance of {@link KryoRegistrar} to do actual {@link Kryo} registration. But since
   * every other instance of the same implementation of {@link KryoRegistrar} should do the same
   * classes registration, we use {@link IdentifiedRegistrar IdentifiedRegistrar's} Id as a key.
   *
   * {@link ThreadLocal} is utilized to allow re-usability of {@link Kryo} by many instances of
   * {@link KryoCoder}.
   *
   */
  private static Map<Integer, ThreadLocal<Kryo>> kryoByRegistrarId =
      new HashMap<>();


  /**
   * Returns {@link Kryo} instance which has classes registered by this {@code registrar} or
   * previously given {@link KryoRegistrar} instance of the same type. The returned instance is
   * either created by this call or returned from cache.
   * <p>
   * If given {@code registrar} is {@link #NO_OP_REGISTRAR} then returned kryo allows for
   * (de)serialization of unregistered classes. That is not otherwise allowed.
   * </p>
   */
  static Kryo getOrCreateKryo(IdentifiedRegistrar registrarWithId) {
    Objects.requireNonNull(registrarWithId);

    synchronized (kryoByRegistrarId) {

      ThreadLocal<Kryo> kryoThreadLocal = kryoByRegistrarId
          .computeIfAbsent(registrarWithId.getId(), (k) -> new ThreadLocal<>());

      Kryo kryoInstance = kryoThreadLocal.get();
      if (kryoInstance == null) {
        kryoInstance = createKryo(registrarWithId);
        kryoThreadLocal.set(kryoInstance);
      }

      return kryoInstance;
    }
  }

  static Input getKryoInput() {
    return threadLocalInput.get();
  }

  static Output getKryoOutput() {
    return threadLocalOutput.get();
  }

}
