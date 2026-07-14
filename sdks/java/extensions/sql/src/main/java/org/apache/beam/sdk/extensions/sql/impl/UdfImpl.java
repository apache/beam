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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.lang.reflect.Method;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.TranslatableTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.TableMacroImpl;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Beam-customized facade behind {@link Function} to address BEAM-5921. */
class UdfImpl {

  private UdfImpl() {}

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function} from
   * given class.
   *
   * <p>If a method of the given name is not found or it does not suit, returns {@code null}.
   *
   * @param clazz class that is used to implement the function
   * @param methodName Method name (typically "eval")
   * @return created {@link Function}
   */
  public static Function create(Class<?> clazz, String methodName) {
    final @Nullable Method method = findMethod(clazz, methodName);

    if (method == null) {
      throw new RuntimeException(
          String.format(
              "Cannot create UDF from method: method %s.%s not found",
              clazz.getCanonicalName(), methodName));
    }

    return create(method);
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function} from
   * given method.
   *
   * @param method method that is used to implement the function
   * @return created {@link Function} or null
   */
  public static Function create(Method method) {
    if (TranslatableTable.class.isAssignableFrom(method.getReturnType())) {
      return checkArgumentNotNull(
          TableMacroImpl.create(method), "Could not create function from method: %s", method);
    } else {
      return ScalarFunctionImpl.create(method);
    }
  }

  /*
   * Finds a method in a given class by name. In case of overloaded methods with the same name,
   * this prioritizes the overload with the maximum number of parameters. This ensures Calcite
   * can resolve optional/default trailing parameters correctly when binding UDF overloads.
   *
   * @param clazz class to search method in
   * @param name name of the method to find
   * @return the matching method with the highest parameter count or null when no method found
   */
  static @Nullable Method findMethod(Class<?> clazz, String name) {
    Method bestMethod = null;
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name) && !method.isBridge()) {
        if (bestMethod == null) {
          bestMethod = method;
        } else {
          int cmp = Integer.compare(method.getParameterCount(), bestMethod.getParameterCount());
          if (cmp > 0 || (cmp == 0 && method.toString().compareTo(bestMethod.toString()) < 0)) {
            bestMethod = method;
          }
        }
      }
    }
    return bestMethod;
  }
}
