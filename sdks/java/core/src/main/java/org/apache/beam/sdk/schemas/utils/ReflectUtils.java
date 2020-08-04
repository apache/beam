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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimaps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Primitives;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A set of reflection helper methods. */
@Internal
public class ReflectUtils {
  /** Represents a class and a schema. */
  @AutoValue
  @Experimental(Kind.SCHEMAS)
  public abstract static class ClassWithSchema {
    public abstract Class getClazz();

    public abstract Schema getSchema();

    public static ClassWithSchema create(Class clazz, Schema schema) {
      return new AutoValue_ReflectUtils_ClassWithSchema(clazz, schema);
    }
  }

  private static final Map<Class, List<Method>> DECLARED_METHODS = Maps.newConcurrentMap();
  private static final Map<Class, Method> ANNOTATED_CONSTRUCTORS = Maps.newConcurrentMap();
  private static final Map<Class, List<Field>> DECLARED_FIELDS = Maps.newConcurrentMap();

  /**
   * Returns the list of non private/protected, non-static methods in the class, caching the
   * results.
   */
  public static List<Method> getMethods(Class clazz) {
    return DECLARED_METHODS.computeIfAbsent(
        clazz,
        c -> {
          return Arrays.stream(c.getDeclaredMethods())
              .filter(
                  m -> !m.isBridge()) // Covariant overloads insert bridge functions, which we must
              // ignore.
              .filter(m -> !Modifier.isPrivate(m.getModifiers()))
              .filter(m -> !Modifier.isProtected(m.getModifiers()))
              .filter(m -> !Modifier.isStatic(m.getModifiers()))
              .collect(Collectors.toList());
        });
  }

  public static Multimap<String, Method> getMethodsMap(Class clazz) {
    return Multimaps.index(getMethods(clazz), Method::getName);
  }

  public static @Nullable Constructor getAnnotatedConstructor(Class clazz) {
    return Arrays.stream(clazz.getDeclaredConstructors())
        .filter(m -> !Modifier.isPrivate(m.getModifiers()))
        .filter(m -> !Modifier.isProtected(m.getModifiers()))
        .filter(m -> m.getAnnotation(SchemaCreate.class) != null)
        .findFirst()
        .orElse(null);
  }

  public static @Nullable Method getAnnotatedCreateMethod(Class clazz) {
    return ANNOTATED_CONSTRUCTORS.computeIfAbsent(
        clazz,
        c -> {
          Method method =
              Arrays.stream(clazz.getDeclaredMethods())
                  .filter(m -> !Modifier.isPrivate(m.getModifiers()))
                  .filter(m -> !Modifier.isProtected(m.getModifiers()))
                  .filter(m -> Modifier.isStatic(m.getModifiers()))
                  .filter(m -> m.getAnnotation(SchemaCreate.class) != null)
                  .findFirst()
                  .orElse(null);
          if (method != null && !clazz.isAssignableFrom(method.getReturnType())) {
            throw new InvalidParameterException(
                "A method marked with SchemaCreate in class "
                    + clazz
                    + " does not return a type assignable to "
                    + clazz);
          }
          return method;
        });
  }

  // Get all public, non-static, non-transient fields.
  public static List<Field> getFields(Class<?> clazz) {
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
                if ((field.getModifiers() & (Modifier.PRIVATE | Modifier.PROTECTED)) == 0) {
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

  public static boolean isGetter(Method method) {
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

  public static boolean isSetter(Method method) {
    return method.getParameterCount() == 1 && method.getName().startsWith("set");
  }

  public static String stripPrefix(String methodName, String prefix) {
    if (!methodName.startsWith(prefix)) {
      return methodName;
    }
    String firstLetter = methodName.substring(prefix.length(), prefix.length() + 1).toLowerCase();

    return (methodName.length() == prefix.length() + 1)
        ? firstLetter
        : (firstLetter + methodName.substring(prefix.length() + 1, methodName.length()));
  }

  public static String stripGetterPrefix(String method) {
    if (method.startsWith("get")) {
      return stripPrefix(method, "get");
    }
    return stripPrefix(method, "is");
  }

  public static String stripSetterPrefix(String method) {
    return stripPrefix(method, "set");
  }

  /** For an array T[] or a subclass of Iterable<T>, return a TypeDescriptor describing T. */
  public static @Nullable TypeDescriptor getIterableComponentType(TypeDescriptor valueType) {
    TypeDescriptor componentType = null;
    if (valueType.isArray()) {
      Type component = valueType.getComponentType().getType();
      if (!component.equals(byte.class)) {
        // Byte arrays are special cased since we have a schema type corresponding to them.
        componentType = TypeDescriptor.of(component);
      }
    } else if (valueType.isSubtypeOf(TypeDescriptor.of(Iterable.class))) {
      TypeDescriptor<Iterable<?>> collection = valueType.getSupertype(Iterable.class);
      if (collection.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) collection.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        checkArgument(params.length == 1);
        componentType = TypeDescriptor.of(params[0]);
      } else {
        throw new RuntimeException("Collection parameter is not parameterized!");
      }
    }
    return componentType;
  }

  public static TypeDescriptor getMapType(TypeDescriptor valueType, int index) {
    TypeDescriptor mapType = null;
    if (valueType.isSubtypeOf(TypeDescriptor.of(Map.class))) {
      TypeDescriptor<Collection<?>> map = valueType.getSupertype(Map.class);
      if (map.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) map.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        mapType = TypeDescriptor.of(params[index]);
      } else {
        throw new RuntimeException("Map type is not parameterized! " + map);
      }
    }
    return mapType;
  }

  public static TypeDescriptor boxIfPrimitive(TypeDescriptor typeDescriptor) {
    return typeDescriptor.getRawType().isPrimitive()
        ? TypeDescriptor.of(Primitives.wrap(typeDescriptor.getRawType()))
        : typeDescriptor;
  }
}
