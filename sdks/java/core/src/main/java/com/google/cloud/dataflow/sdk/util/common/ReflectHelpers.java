/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.common;

import static java.util.Arrays.asList;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Queue;

import javax.annotation.Nullable;

/**
 * Utilities for working with with {@link Class Classes} and {@link Method Methods}.
 */
public class ReflectHelpers {

  private static final Joiner COMMA_SEPARATOR = Joiner.on(", ");

  /** A {@link Function} that turns a method into a simple method signature. */
  public static final Function<Method, String> METHOD_FORMATTER = new Function<Method, String>() {
    @Override
    public String apply(Method input) {
      String parameterTypes = FluentIterable.from(asList(input.getParameterTypes()))
          .transform(CLASS_SIMPLE_NAME)
          .join(COMMA_SEPARATOR);
      return String.format("%s(%s)",
          input.getName(),
          parameterTypes);
    }
  };

  /** A {@link Function} that turns a method into the declaring class + method signature. */
  public static final Function<Method, String> CLASS_AND_METHOD_FORMATTER =
      new Function<Method, String>() {
    @Override
    public String apply(Method input) {
      return String.format("%s#%s",
          CLASS_NAME.apply(input.getDeclaringClass()),
          METHOD_FORMATTER.apply(input));
    }
  };

  /** A {@link Function} with returns the classes name. */
  public static final Function<Class<?>, String> CLASS_NAME =
      new Function<Class<?>, String>() {
    @Override
    public String apply(Class<?> input) {
      return input.getName();
    }
  };

  /** A {@link Function} with returns the classes name. */
  public static final Function<Class<?>, String> CLASS_SIMPLE_NAME =
      new Function<Class<?>, String>() {
    @Override
    public String apply(Class<?> input) {
      return input.getSimpleName();
    }
  };

  /** A {@link Function} that formats types. */
  public static final Function<Type, String> TYPE_SIMPLE_DESCRIPTION =
      new Function<Type, String>() {
    @Override
    @Nullable
    public String apply(@Nullable Type input) {
      StringBuilder builder = new StringBuilder();
      format(builder, input);
      return builder.toString();
    }

    private void format(StringBuilder builder, Type t) {
      if (t instanceof Class) {
        formatClass(builder, (Class<?>) t);
      } else if (t instanceof TypeVariable) {
        formatTypeVariable(builder, (TypeVariable<?>) t);
      } else if (t instanceof WildcardType) {
        formatWildcardType(builder, (WildcardType) t);
      } else if (t instanceof ParameterizedType) {
        formatParameterizedType(builder, (ParameterizedType) t);
      } else if (t instanceof GenericArrayType) {
        formatGenericArrayType(builder, (GenericArrayType) t);
      } else {
        builder.append(t.toString());
      }
    }

    private void formatClass(StringBuilder builder, Class<?> clazz) {
      builder.append(clazz.getSimpleName());
    }

    private void formatTypeVariable(StringBuilder builder, TypeVariable<?> t) {
      builder.append(t.getName());
    }

    private void formatWildcardType(StringBuilder builder, WildcardType t) {
      builder.append("?");
      for (Type lowerBound : t.getLowerBounds()) {
        builder.append(" super ");
        format(builder, lowerBound);
      }
      for (Type upperBound : t.getUpperBounds()) {
        if (!Object.class.equals(upperBound)) {
          builder.append(" extends ");
          format(builder, upperBound);
        }
      }
    }

    private void formatParameterizedType(StringBuilder builder, ParameterizedType t) {
      format(builder, t.getRawType());
      builder.append('<');
      COMMA_SEPARATOR.appendTo(builder,
          FluentIterable.from(asList(t.getActualTypeArguments()))
          .transform(TYPE_SIMPLE_DESCRIPTION));
      builder.append('>');
    }

    private void formatGenericArrayType(StringBuilder builder, GenericArrayType t) {
      format(builder, t.getGenericComponentType());
      builder.append("[]");
    }
  };

  /**
   * Returns all interfaces of the given clazz.
   * @param clazz
   * @return
   */
  public static FluentIterable<Class<?>> getClosureOfInterfaces(Class<?> clazz) {
    Preconditions.checkNotNull(clazz);
    Queue<Class<?>> interfacesToProcess = Queues.newArrayDeque();
    Collections.addAll(interfacesToProcess, clazz.getInterfaces());

    LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();
    while (!interfacesToProcess.isEmpty()) {
      Class<?> current = interfacesToProcess.remove();
      if (interfaces.add(current)) {
        Collections.addAll(interfacesToProcess, current.getInterfaces());
      }
    }
    return FluentIterable.from(interfaces);
  }

  /**
   * Returns all the methods visible from the provided interfaces.
   *
   * @param interfaces The interfaces to use when searching for all their methods.
   * @return An iterable of {@link Method}s which interfaces expose.
   */
  public static Iterable<Method> getClosureOfMethodsOnInterfaces(
      Iterable<? extends Class<?>> interfaces) {
    return FluentIterable.from(interfaces).transformAndConcat(
        new Function<Class<?>, Iterable<Method>>() {
          @Override
          public Iterable<Method> apply(Class<?> input) {
            return getClosureOfMethodsOnInterface(input);
          }
    });
  }

  /**
   * Returns all the methods visible from {@code iface}.
   *
   * @param iface The interface to use when searching for all its methods.
   * @return An iterable of {@link Method}s which {@code iface} exposes.
   */
  public static Iterable<Method> getClosureOfMethodsOnInterface(Class<?> iface) {
    Preconditions.checkNotNull(iface);
    Preconditions.checkArgument(iface.isInterface());
    ImmutableSet.Builder<Method> builder = ImmutableSet.builder();
    Queue<Class<?>> interfacesToProcess = Queues.newArrayDeque();
    interfacesToProcess.add(iface);
    while (!interfacesToProcess.isEmpty()) {
      Class<?> current = interfacesToProcess.remove();
      builder.add(current.getMethods());
      interfacesToProcess.addAll(Arrays.asList(current.getInterfaces()));
    }
    return builder.build();
  }
}
