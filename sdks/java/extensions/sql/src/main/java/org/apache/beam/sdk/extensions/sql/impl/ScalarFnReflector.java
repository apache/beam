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
package org.apache.beam.sdk.extensions.sql.impl;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.extensions.sql.udf.ScalarFn;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** Reflection-based implementation logic for {@link ScalarFn}. */
public class ScalarFnReflector {
  /**
   * Gets the method annotated with {@link
   * org.apache.beam.sdk.extensions.sql.udf.ScalarFn.ApplyMethod} from {@code scalarFn}.
   *
   * <p>There must be exactly one method annotated with {@link
   * org.apache.beam.sdk.extensions.sql.udf.ScalarFn.ApplyMethod}, and it must be public.
   */
  public static Method getApplyMethod(ScalarFn scalarFn) {
    Class<? extends ScalarFn> clazz = scalarFn.getClass();
    Collection<Method> matches =
        ReflectHelpers.declaredMethodsWithAnnotation(
            ScalarFn.ApplyMethod.class, clazz, ScalarFn.class);

    if (matches.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "No method annotated with @%s found in class %s.",
              ScalarFn.ApplyMethod.class.getSimpleName(), clazz.getName()));
    }

    // If we have at least one match, then either it should be the only match
    // or it should be an extension of the other matches (which came from parent
    // classes).
    Method first = matches.iterator().next();
    for (Method other : matches) {
      if (!first.getName().equals(other.getName())
          || !Arrays.equals(first.getParameterTypes(), other.getParameterTypes())) {
        throw new IllegalArgumentException(
            String.format(
                "Found multiple methods annotated with @%s. [%s] and [%s]",
                ScalarFn.ApplyMethod.class.getSimpleName(),
                ReflectHelpers.formatMethod(first),
                ReflectHelpers.formatMethod(other)));
      }
    }

    // Method must be public.
    if ((first.getModifiers() & Modifier.PUBLIC) == 0) {
      throw new IllegalArgumentException(
          String.format("Method %s is not public.", ReflectHelpers.formatMethod(first)));
    }

    return first;
  }
}
