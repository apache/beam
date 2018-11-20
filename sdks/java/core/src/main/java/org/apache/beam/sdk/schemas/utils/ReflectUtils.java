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
package org.apache.beam.sdk.schemas.utils;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;

/** A set of reflection helper methods. */
public class ReflectUtils {
  static class ClassWithSchema {
    private final Class clazz;
    private final Schema schema;

    public ClassWithSchema(Class clazz, Schema schema) {
      this.clazz = clazz;
      this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClassWithSchema that = (ClassWithSchema) o;
      return Objects.equals(clazz, that.clazz) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clazz, schema);
    }
  }

  private static final Map<Class, List<Method>> DECLARED_METHODS = Maps.newHashMap();
  private static final Map<Class, List<Field>> DECLARED_FIELDS = Maps.newHashMap();

  /** Returns the list of public, non-static methods in the class, caching the results. */
  static List<Method> getMethods(Class clazz) throws IOException {
    return DECLARED_METHODS.computeIfAbsent(
        clazz,
        c -> {
          return Arrays.stream(c.getDeclaredMethods())
              .filter(m -> Modifier.isPublic(m.getModifiers()))
              .filter(m -> !Modifier.isStatic(m.getModifiers()))
              .collect(Collectors.toList());
        });
  }

  // Get all public, non-static, non-transient fields.
  static List<Field> getFields(Class<?> clazz) {
    return DECLARED_FIELDS.computeIfAbsent(
        clazz,
        c -> {
          Map<String, Field> types = new LinkedHashMap<>();
          do {
            if (c.getPackage() != null && c.getPackage().getName().startsWith("java.")) {
              break; // skip java built-in classes
            }
            for (java.lang.reflect.Field field : c.getDeclaredFields()) {
              if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
                if ((field.getModifiers() & Modifier.PUBLIC) != 0) {
                  boolean nullable = field.getAnnotation(Nullable.class) != null;
                  checkArgument(
                      types.put(field.getName(), field) == null,
                      c.getSimpleName() + " contains two fields named: " + field);
                }
              }
            }
            c = c.getSuperclass();
          } while (c != null);
          return Lists.newArrayList(types.values());
        });
  }

  static boolean isGetter(Method method) {
    if (Void.TYPE.equals(method.getReturnType())) {
      return false;
    }
    if (method.getName().startsWith("get") && method.getName().length() > 3) {
      return true;
    }
    return (method.getName().startsWith("is")
        && method.getName().length() > 2
        && method.getParameterCount() == 0
        && (Boolean.TYPE.equals(method.getReturnType())
            || Boolean.class.equals(method.getReturnType())));
  }

  static boolean isSetter(Method method) {
    return Void.TYPE.equals(method.getReturnType())
        && method.getParameterCount() == 1
        && method.getName().startsWith("set");
  }

  static String stripPrefix(String methodName, String prefix) {
    String firstLetter = methodName.substring(prefix.length(), prefix.length() + 1).toLowerCase();

    return (methodName.length() == prefix.length() + 1)
        ? firstLetter
        : (firstLetter + methodName.substring(prefix.length() + 1, methodName.length()));
  }
}
