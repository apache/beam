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
package org.apache.beam.sdk.values;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Captures a free type variable that can be used in {@link TypeDescriptor#where}. For example:
 *
 * <pre>{@code
 * static <T> TypeDescriptor<List<T>> listOf(Class<T> elementType) {
 *   return new TypeDescriptor<List<T>>() {}
 *       .where(new TypeParameter<T>() {}, elementType);
 * }
 * }</pre>
 */
public abstract class TypeParameter<T> {
  final TypeVariable<?> typeVariable;

  public TypeParameter() {
    Type superclass = getClass().getGenericSuperclass();
    checkArgument(superclass instanceof ParameterizedType, "%s isn't parameterized", superclass);
    typeVariable = (TypeVariable<?>) ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  @Override
  public int hashCode() {
    return typeVariable.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof TypeParameter)) {
      return false;
    }
    TypeParameter<?> that = (TypeParameter<?>) obj;
    return typeVariable.equals(that.typeVariable);
  }

  @Override
  public String toString() {
    return typeVariable.toString();
  }
}
