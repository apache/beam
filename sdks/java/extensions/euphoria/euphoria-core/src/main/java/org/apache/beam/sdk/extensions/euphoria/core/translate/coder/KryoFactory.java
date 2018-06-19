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
   * No-op registrar. Use of this registrar degrades performance since {@link Kryo} needs to
   * serialize fully specified class name instead of id.
   * <p>
   * {@link #getOrCreateKryo(KryoRegistrar)} returns {@link Kryo}  which allows for serialization of
   * unregistered classes when this {@link KryoRegistrar} is used to call it.
   * </p>
   */
  static final KryoRegistrar NO_OP_REGISTRAR = new KryoRegistrar() {
    // this needs to be anonymous implementation in order to keep its type after serialization
    @Override
    public void registerClasses(Kryo kryo) {
      //No-Op
    }
  };


  private static Kryo createKryo(KryoRegistrar registrar) {
    final Kryo instance = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) instance.getInstantiatorStrategy())
        .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    if (NO_OP_REGISTRAR.getClass().equals(registrar.getClass())) {
      instance.setRegistrationRequired(false);
    } else {
      instance.setRegistrationRequired(true);
      registrar.registerClasses(instance);
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
   * classes registration, we use {@code Class<? extends KryoRegistrar>} as a key.
   * <p>
   * {@link ThreadLocal} us utilized to allow re-usability of {@link Kryo} by many instnces of
   * {@link KryoCoder}.
   * </p>
   */
  private static Map<Class<? extends KryoRegistrar>, ThreadLocal<Kryo>> kryoByRegistrarType =
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
  static Kryo getOrCreateKryo(KryoRegistrar registrar) {
    Objects.requireNonNull(registrar);

    synchronized (kryoByRegistrarType) {

      ThreadLocal<Kryo> kryoThreadLocal = kryoByRegistrarType
          .computeIfAbsent(registrar.getClass(), (k) -> new ThreadLocal<>());

      Kryo kryoInstance = kryoThreadLocal.get();
      if (kryoInstance == null) {
        kryoInstance = createKryo(registrar);
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
