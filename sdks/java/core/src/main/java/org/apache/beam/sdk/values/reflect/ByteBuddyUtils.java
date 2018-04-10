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

package org.apache.beam.sdk.values.reflect;

import static net.bytebuddy.matcher.ElementMatchers.named;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;

/**
 * Utilities to help with code generation for implementing {@link FieldValueGetter}s.
 */
class ByteBuddyUtils {

  /**
   * Creates an instance of the {@link DynamicType.Builder}
   * to start implementation of the {@link FieldValueGetter}.
   */
  static DynamicType.Builder<FieldValueGetter> subclassGetterInterface(
      ByteBuddy byteBuddy, Class clazz) {

    TypeDescription.Generic getterGenericType =
        TypeDescription.Generic.Builder.parameterizedType(FieldValueGetter.class, clazz).build();

    return (DynamicType.Builder<FieldValueGetter>) byteBuddy.subclass(getterGenericType);
  }

  /**
   * Implements {@link FieldValueGetter#name()}.
   */
  static DynamicType.Builder<FieldValueGetter> implementNameGetter(
      DynamicType.Builder<FieldValueGetter> getterClassBuilder,
      String fieldName) {

    return getterClassBuilder
        .method(named("name"))
        .intercept(FixedValue.reference(fieldName));
  }

  /**
   * Implements {@link FieldValueGetter#type()}.
   */
  static DynamicType.Builder<FieldValueGetter> implementTypeGetter(
      DynamicType.Builder<FieldValueGetter> getterClassBuilder,
      Class fieldType) {

    return getterClassBuilder
        .method(named("type"))
        .intercept(FixedValue.reference(fieldType));
  }

  /**
   * Implements {@link FieldValueGetter#get(Object)} for getting public fields from pojos.
   */
  static DynamicType.Builder<FieldValueGetter> implementValueGetter(
      DynamicType.Builder<FieldValueGetter> getterClassBuilder,
      Implementation fieldAccessImplementation) {

    return getterClassBuilder
        .method(named("get"))
        .intercept(fieldAccessImplementation);
  }

  /**
   * Finish the {@link FieldValueGetter} implementation and return its new instance.
   *
   * <p>Wraps underlying {@link InstantiationException} and {@link IllegalAccessException}
   * into {@link RuntimeException}.
   *
   * <p>Does no validations of whether everything has been implemented correctly.
   */
  static FieldValueGetter makeNewGetterInstance(
      String fieldName,
      DynamicType.Builder<FieldValueGetter> getterBuilder) {

    try {
      return getterBuilder
          .make()
          .load(
              ByteBuddyUtils.class.getClassLoader(),
              ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(
          "Unable to generate a getter for field '" + fieldName + "'.", e);
    }
  }
}
