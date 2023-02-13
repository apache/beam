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
package org.apache.beam.sdk.util;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Method;
import net.bytebuddy.dynamic.loading.ClassInjector;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** Utilities for working with Byte Buddy. */
public final class ByteBuddyUtils {
  private ByteBuddyUtils() {} // Non-instantiable

  /** Returns a class loading strategy that is compatible with Java 17+. */
  public static ClassLoadingStrategy<ClassLoader> getClassLoadingStrategy(Class<?> targetClass) {
    try {
      ClassLoadingStrategy<ClassLoader> strategy;
      if (ClassInjector.UsingLookup.isAvailable()) {
        ClassLoader classLoader = ReflectHelpers.findClassLoader(targetClass);
        Class<?> methodHandles = Class.forName("java.lang.invoke.MethodHandles", true, classLoader);
        @SuppressWarnings("nullness") // MethodHandles#lookup accepts null
        Object lookup = methodHandles.getMethod("lookup").invoke(null);
        Class<?> lookupClass =
            Class.forName("java.lang.invoke.MethodHandles$Lookup", true, classLoader);
        Method privateLookupIn =
            methodHandles.getMethod("privateLookupIn", Class.class, lookupClass);
        @SuppressWarnings("nullness") // this is a static method, the receiver can be null
        Object privateLookup = requireNonNull(privateLookupIn.invoke(null, targetClass, lookup));
        strategy = ClassLoadingStrategy.UsingLookup.of(requireNonNull(privateLookup));
      } else if (ClassInjector.UsingReflection.isAvailable()) {
        strategy = ClassLoadingStrategy.Default.INJECTION;
      } else {
        throw new IllegalStateException("No code generation strategy available");
      }
      return strategy;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("No code generation strategy available", e);
    }
  }
}
