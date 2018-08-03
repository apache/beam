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

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link CoderProvider}, backbone of {@link RegisterCoders} API and {@link Kryo}
 * integration.
 */
public class EuphoriaCoderProvider extends CoderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(EuphoriaCoderProvider.class);

  private final Map<TypeDescriptor, Coder<?>> typeToCoder;
  private final Map<Class<?>, Coder<?>> classToCoder;
  private final IdentifiedRegistrar kryoRegistrar;

  EuphoriaCoderProvider(
      Map<TypeDescriptor, Coder<?>> typeToCoder,
      Map<Class<?>, Coder<?>> classToCoder,
      KryoRegistrar kryoRegistrar) {
    this.typeToCoder = typeToCoder;
    this.classToCoder = classToCoder;
    if (kryoRegistrar != null) {
      this.kryoRegistrar = IdentifiedRegistrar.of(kryoRegistrar);
    } else {
      this.kryoRegistrar = null;
    }
  }

  @Override
  public <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
      throws CannotProvideCoderException {

    // try to obtain most specific coder by type descriptor
    Coder<?> coder = typeToCoder.get(typeDescriptor);

    // second try, obtain coder by raw encoding type
    if (coder == null) {
      Class<? super T> rawType = typeDescriptor.getRawType();
      coder = classToCoder.get(rawType);

      // if we still do not have a coder check whenever given class was registered with kryo
      if (coder == null) {
        coder = createKryoCoderIfClassRegistered(rawType);
      }
    }

    if (coder == null) {
      LOG.info(String.format("%s cannot provide coder for '%s'", this, typeDescriptor));
      throw new CannotProvideCoderException(
          String.format("No coder for given type descriptor '%s' found.", typeDescriptor));
    }

    @SuppressWarnings("unchecked")
    Coder<T> castedCoder = (Coder<T>) coder;

    return castedCoder;
  }

  private <T> Coder<T> createKryoCoderIfClassRegistered(Class<? super T> rawType) {

    if (kryoRegistrar == null) {
      return null;
    }

    Kryo kryo = KryoFactory.getOrCreateKryo(kryoRegistrar);
    ClassResolver classResolver = kryo.getClassResolver();

    Registration registration = classResolver.getRegistration(rawType);
    if (registration == null) {
      return null;
    }

    Coder<T> coder = createKryoCoderWithRegisteredClasses();
    classToCoder.put(rawType, coder);

    return coder;
  }

  public <T> KryoCoder<T> createKryoCoderWithRegisteredClasses() {
    return KryoCoder.of(kryoRegistrar);
  }
}
