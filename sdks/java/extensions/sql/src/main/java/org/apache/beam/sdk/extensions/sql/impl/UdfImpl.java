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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.TranslatableTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.impl.TableMacroImpl;

/** Beam-customized facade behind {@link Function} to address BEAM-5921. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class UdfImpl {

  private UdfImpl() {}

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function} from
   * given class.
   *
   * <p>If a method of the given name is not found or it does not suit, returns {@code null}.
   *
   * @param clazz class that is used to implement the function
   * @param methodName Method name (typically "eval")
   * @return created {@link Function} or null
   */
  public static Function create(Class<?> clazz, String methodName) {
    final Method method = findMethod(clazz, methodName);
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function} from
   * given method.
   *
   * @param method method that is used to implement the function
   * @return created {@link Function} or null
   */
  public static Function create(Method method) {
    if (TranslatableTable.class.isAssignableFrom(method.getReturnType())) {
      return TableMacroImpl.create(method);
    } else {
      return ScalarFunctionImpl.create(method);
    }
  }

  /*
   * Finds a method in a given class by name.
   * @param clazz class to search method in
   * @param name name of the method to find
   * @return the first method with matching name or null when no method found
   */
  static Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name) && !method.isBridge()) {
        return method;
      }
    }
    return null;
  }
}
