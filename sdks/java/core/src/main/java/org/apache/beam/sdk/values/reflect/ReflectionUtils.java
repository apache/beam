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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/** Helpers to get the information about a class. */
class ReflectionUtils {

  /** Returns a list of non-void public methods with names prefixed with 'get'. */
  static List<Method> getPublicGetters(Class clazz) {
    List<Method> getters = new ArrayList<>();
    for (Method method : clazz.getDeclaredMethods()) {
      if (isGetter(method) && isPublic(method)) {
        getters.add(method);
      }
    }

    return getters;
  }

  /**
   * Tries to remove a 'get' prefix from a method name.
   *
   * <p>Converts method names like 'getSomeField' into 'someField' if they start with 'get'. Returns
   * names unchanged if they don't start with 'get'.
   */
  static String tryStripGetPrefix(Method method) {
    String name = method.getName();

    if (name.length() <= 3 || !name.startsWith("get")) {
      return name;
    }

    String firstLetter = name.substring(3, 4).toLowerCase();

    return (name.length() == 4) ? firstLetter : (firstLetter + name.substring(4, name.length()));
  }

  private static boolean isGetter(Method method) {
    return method.getName().startsWith("get") && !Void.TYPE.equals(method.getReturnType());
  }

  private static boolean isPublic(Method method) {
    return Modifier.isPublic(method.getModifiers());
  }
}
