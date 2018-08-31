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
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KryoRegistrar} enriched by Id.
 *
 * <p>New instances of the same (possibly lambda) implementation of {@link KryoRegistrar} may be
 * created by (de)serialization. And since lambda expressions do not retain their type (instance of
 * {@link Class}) after deserialization, we need something else to avoid creation of more {@link
 * Kryo} instances then really needed. That is why any given {@link KryoRegistrar} instance is
 * enriched by Id.
 */
class IdentifiedRegistrar implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterCoders.class);

  private static final AtomicInteger idSource = new AtomicInteger();

  private final int id;
  private final KryoRegistrar registrar;

  private IdentifiedRegistrar(int id, KryoRegistrar registrar) {
    this.id = id;
    this.registrar = registrar;
  }

  static IdentifiedRegistrar of(KryoRegistrar registrar) {
    Objects.requireNonNull(registrar);
    IdentifiedRegistrar identifiedRegistrar =
        new IdentifiedRegistrar(idSource.getAndIncrement(), registrar);
    LOG.info(
        String.format(
            "Id: '%d' was assigned to given %s of type '%s'.",
            identifiedRegistrar.getId(),
            KryoRegistrar.class.getSimpleName(),
            registrar.getClass()));
    return identifiedRegistrar;
  }

  @Override
  public String toString() {
    return "IdentifiedRegistrar{" + "id=" + id + ", registrar=" + registrar + '}';
  }

  public int getId() {
    return id;
  }

  public KryoRegistrar getRegistrar() {
    return registrar;
  }
}
