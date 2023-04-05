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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import java.lang.reflect.Method;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.ScalarFunction;

/** ZetaSQL-specific extension to {@link ScalarFunctionImpl}. */
public class ZetaSqlScalarFunctionImpl extends ScalarFunctionImpl {
  /**
   * ZetaSQL function group identifier. Different function groups may have divergent translation
   * paths.
   */
  public final String functionGroup;

  private ZetaSqlScalarFunctionImpl(
      Method method, CallImplementor implementor, String functionGroup, String jarPath) {
    super(method, implementor, jarPath);
    this.functionGroup = functionGroup;
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function} from
   * given class.
   *
   * <p>If a method of the given name is not found or it does not suit, returns {@code null}.
   *
   * @param clazz class that is used to implement the function
   * @param methodName Method name (typically "eval")
   * @param functionGroup ZetaSQL function group identifier. Different function groups may have
   *     divergent translation paths.
   * @return created {@link ScalarFunction} or null
   */
  public static Function create(
      Class<?> clazz, String methodName, String functionGroup, String jarPath) {
    return create(findMethod(clazz, methodName), functionGroup, jarPath);
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Function} from
   * given method. When {@code eval} method does not suit, {@code null} is returned.
   *
   * @param method method that is used to implement the function
   * @param functionGroup ZetaSQL function group identifier. Different function groups may have
   *     divergent translation paths.
   * @return created {@link Function} or null
   */
  public static Function create(Method method, String functionGroup, String jarPath) {
    validateMethod(method);
    CallImplementor implementor = createImplementor(method);
    return new ZetaSqlScalarFunctionImpl(method, implementor, functionGroup, jarPath);
  }

  /*
   * Finds a method in a given class by name.
   * @param clazz class to search method in
   * @param name name of the method to find
   * @return the first method with matching name or null when no method found
   */
  private static Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name) && !method.isBridge()) {
        return method;
      }
    }
    throw new NoSuchMethodError(
        String.format("Method %s not found in class %s.", name, clazz.getName()));
  }
}
