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

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;

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
    List<FieldValueSetter> setters = setterFactory.create(typeDescriptor, schema);
    return new SchemaUserTypeCreator() {
      @Override
      public Object create(Object... params) {
        Object object;
        try {
          object = typeDescriptor.getRawType().getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException
            | IllegalAccessException
            | InvocationTargetException
            | InstantiationException e) {
          throw new RuntimeException("Failed to instantiate object ", e);
        }
        for (int i = 0; i < params.length; ++i) {
          FieldValueSetter setter = setters.get(i);
          setter.set(object, params[i]);
        }
        return object;
      }
    };
  }
}
