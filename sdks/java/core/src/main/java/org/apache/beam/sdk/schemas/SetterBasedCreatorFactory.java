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
package org.apache.beam.sdk.schemas;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Factory} that uses a default constructor and a list of setters to construct a {@link
 * SchemaUserTypeCreator}.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class SetterBasedCreatorFactory implements Factory<SchemaUserTypeCreator> {
  private final Factory<List<FieldValueSetter>> setterFactory;

  public SetterBasedCreatorFactory(Factory<List<FieldValueSetter>> setterFactory) {
    this.setterFactory = new CachingFactory<>(setterFactory);
  }

  @Override
  public SchemaUserTypeCreator create(TypeDescriptor<?> typeDescriptor, Schema schema) {
    List<FieldValueSetter> setterList = setterFactory.create(typeDescriptor, schema);
    FieldValueSetter[] setters = setterList.toArray(new FieldValueSetter[0]);
    return new SetterBasedCreator(typeDescriptor, setters);
  }

  /** Serializable creator caching the reflected no-arg constructor in a transient field. */
  private static final class SetterBasedCreator implements SchemaUserTypeCreator {
    private final TypeDescriptor<?> typeDescriptor;
    private final FieldValueSetter[] setters;
    private transient volatile @Nullable Constructor<?> cachedCtor;

    SetterBasedCreator(TypeDescriptor<?> typeDescriptor, FieldValueSetter[] setters) {
      this.typeDescriptor = typeDescriptor;
      this.setters = setters;
      try {
        this.cachedCtor = typeDescriptor.getRawType().getDeclaredConstructor();
      } catch (NoSuchMethodException e) {
        // Preserve deferred failure inside create() for compatibility with lazy callers/tests.
        this.cachedCtor = null;
      }
    }

    @Override
    public Object create(Object... params) {
      Object object;
      try {
        Constructor<?> ctor = cachedCtor;
        if (ctor == null) {
          ctor = typeDescriptor.getRawType().getDeclaredConstructor();
          cachedCtor = ctor;
        }
        object = ctor.newInstance();
      } catch (NoSuchMethodException
          | IllegalAccessException
          | InvocationTargetException
          | InstantiationException e) {
        throw new RuntimeException("Failed to instantiate object ", e);
      }
      for (int i = 0; i < params.length; ++i) {
        setters[i].set(object, params[i]);
      }
      return object;
    }
  }
}
