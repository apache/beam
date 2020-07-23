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
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.OutputChunked;
import java.util.HashMap;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;

/** Reusable kryo instance. */
class KryoState {

  private static final Storage STORAGE = new Storage();

  static KryoState get(KryoCoder<?> coder) {
    return STORAGE.getOrCreate(coder);
  }

  /** Caching thread local storage for reusable {@link KryoState}s. */
  private static class Storage {

    private final ThreadLocal<Map<String, KryoState>> kryoStateMap =
        ThreadLocal.withInitial(HashMap::new);

    KryoState getOrCreate(KryoCoder<?> coder) {
      return kryoStateMap
          .get()
          .computeIfAbsent(
              coder.getInstanceId(),
              k -> {
                final Kryo kryo = new Kryo();
                // fallback in case serialized class does not have default constructor
                kryo.setInstantiatorStrategy(
                    new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
                kryo.setReferences(coder.getOptions().getReferences());
                kryo.setRegistrationRequired(coder.getOptions().getRegistrationRequired());

                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                if (classLoader == null) {
                  throw new RuntimeException(
                          "Cannot detect classpath: classload is null (is it the bootstrap classloader?)");
                }

                kryo.setClassLoader(classLoader);
                // first id of user provided class registration
                final int firstRegistrationId = kryo.getNextRegistrationId();
                // register user provided classes
                for (KryoRegistrar registrar : coder.getRegistrars()) {
                  registrar.registerClasses(kryo);
                }
                return new KryoState(
                    kryo,
                    firstRegistrationId,
                    new InputChunked(coder.getOptions().getBufferSize()),
                    new OutputChunked(coder.getOptions().getBufferSize()));
              });
    }
  }

  /** The kryo instance. */
  private final Kryo kryo;

  /** first id of user provided class registration. */
  private final int firstRegistrationId;

  /** A reusable input buffer. */
  private final InputChunked inputChunked;

  /** A reusable output buffer. */
  private final OutputChunked outputChunked;

  private KryoState(
      Kryo kryo, int firstRegistrationId, InputChunked inputChunked, OutputChunked outputChunked) {
    this.kryo = kryo;
    this.firstRegistrationId = firstRegistrationId;
    this.inputChunked = inputChunked;
    this.outputChunked = outputChunked;
  }

  /**
   * {@link KryoState#kryo}.
   *
   * @return kryo
   */
  Kryo getKryo() {
    return kryo;
  }

  /**
   * {@link KryoState#firstRegistrationId}.
   *
   * @return registration id
   */
  public int getFirstRegistrationId() {
    return firstRegistrationId;
  }

  /**
   * {@link KryoState#inputChunked}.
   *
   * @return input buffer
   */
  InputChunked getInputChunked() {
    return inputChunked;
  }

  /**
   * {@link KryoState#outputChunked}.
   *
   * @return output buffer
   */
  OutputChunked getOutputChunked() {
    return outputChunked;
  }
}
