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
package org.apache.beam.sdk.util.common;

import static java.util.Arrays.asList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Queue;
import java.util.ServiceLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSortedSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Queues;

/** Utilities for working with with {@link Class Classes} and {@link Method Methods}. */
public class ReflectHelpers {

  private static final Joiner COMMA_SEPARATOR = Joiner.on(", ");

  /** Returns a string representation of the signature of a {@link Method}. */
  public static String formatMethod(Method input) {
    String parameterTypes =
        FluentIterable.from(asList(input.getParameterTypes()))
            .transform(Class::getSimpleName)
            .join(COMMA_SEPARATOR);
    return String.format("%s(%s)", input.getName(), parameterTypes);
  }

  /** Returns a string representation of the class + method signature for a {@link Method}. */
  public static String formatMethodWithClass(Method input) {
    return String.format("%s#%s", input.getDeclaringClass().getName(), formatMethod(input));
  }

  /** A {@link Function} that returns a concise string for a {@link Annotation}. */
  public static String formatAnnotation(Annotation annotation) {
    String annotationName = annotation.annotationType().getName();
    String annotationNameWithoutPackage =
        annotationName.substring(annotationName.lastIndexOf('.') + 1).replace('$', '.');
    String annotationToString = annotation.toString();
    String values = annotationToString.substring(annotationToString.indexOf('('));
    return String.format("%s%s", annotationNameWithoutPackage, values);
  }

  /** A {@link Function} that formats types. */
  public static String simpleTypeDescription(Type input) {
    StringBuilder builder = new StringBuilder();
    format(builder, input);
    return builder.toString();
  }

  private static void format(StringBuilder builder, Type t) {
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

  private static void formatClass(StringBuilder builder, Class<?> clazz) {
    builder.append(clazz.getSimpleName());
  }

  private static void formatTypeVariable(StringBuilder builder, TypeVariable<?> t) {
    builder.append(t.getName());
  }

  private static void formatWildcardType(StringBuilder builder, WildcardType t) {
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

  private static void formatParameterizedType(StringBuilder builder, ParameterizedType t) {
    if (t.getOwnerType() != null) {
      format(builder, t.getOwnerType());
      builder.append('.');
    }
    format(builder, t.getRawType());
    if (t.getActualTypeArguments().length > 0) {
      builder.append('<');
      COMMA_SEPARATOR.appendTo(
          builder,
          FluentIterable.from(asList(t.getActualTypeArguments()))
              .transform(ReflectHelpers::simpleTypeDescription));
      builder.append('>');
    }
  }

  private static void formatGenericArrayType(StringBuilder builder, GenericArrayType t) {
    format(builder, t.getGenericComponentType());
    builder.append("[]");
  }

  /** A {@link Comparator} that uses the object's class' canonical name to compare them. */
  public static class ObjectsClassComparator implements Comparator<Object> {
    public static final ObjectsClassComparator INSTANCE = new ObjectsClassComparator();

    @Override
    public int compare(Object o1, Object o2) {
      return o1.getClass().getCanonicalName().compareTo(o2.getClass().getCanonicalName());
    }
  }

  /**
   * Returns all the methods visible from the provided interfaces.
   *
   * @param interfaces The interfaces to use when searching for all their methods.
   * @return An iterable of {@link Method}s which interfaces expose.
   */
  public static Iterable<Method> getClosureOfMethodsOnInterfaces(
      Iterable<? extends Class<?>> interfaces) {
    return FluentIterable.from(interfaces)
        .transformAndConcat(ReflectHelpers::getClosureOfMethodsOnInterface);
  }

  /**
   * Returns all the methods visible from {@code iface}.
   *
   * @param iface The interface to use when searching for all its methods.
   * @return An iterable of {@link Method}s which {@code iface} exposes.
   */
  public static Iterable<Method> getClosureOfMethodsOnInterface(Class<?> iface) {
    checkNotNull(iface);
    checkArgument(iface.isInterface());
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

  /**
   * Returns instances of all implementations of the the specified {@code iface}. Instances are
   * sorted by their class' name to ensure deterministic execution.
   *
   * @param iface The interface to load implementations of
   * @param classLoader The class loader to use
   * @param <T> The type of {@code iface}
   * @return An iterable of instances of T, ordered by their class' canonical name
   */
  public static <T> Iterable<T> loadServicesOrdered(Class<T> iface, ClassLoader classLoader) {
    ServiceLoader<T> loader = ServiceLoader.load(iface, classLoader);
    ImmutableSortedSet.Builder<T> builder =
        new ImmutableSortedSet.Builder<>(ObjectsClassComparator.INSTANCE);
    builder.addAll(loader);
    return builder.build();
  }

  /**
   * A version of {@code loadServicesOrdered} that uses a default class loader.
   *
   * @param iface The interface to load implementations of
   * @param <T> The type of {@code iface}
   * @return An iterable of instances of T, ordered by their class' canonical name
   */
  public static <T> Iterable<T> loadServicesOrdered(Class<T> iface) {
    return loadServicesOrdered(iface, ReflectHelpers.findClassLoader());
  }

  /**
   * Finds the appropriate {@code ClassLoader} to be used by the {@link ServiceLoader#load} call,
   * which by default would use the proposed {@code ClassLoader}, which can be null. The fallback is
   * as follows: context ClassLoader, class ClassLoader and finaly the system ClassLoader.
   */
  public static ClassLoader findClassLoader(final ClassLoader proposed) {
    ClassLoader classLoader = proposed;
    if (classLoader == null) {
      classLoader = ReflectHelpers.class.getClassLoader();
    }
    if (classLoader == null) {
      classLoader = ClassLoader.getSystemClassLoader();
    }
    return classLoader;
  }

  /** Find the common classloader of all these classes. */
  public static ClassLoader findClassLoader(final Class<?>... classes) {
    if (classes == null || classes.length == 0) {
      throw new IllegalArgumentException("set of classes can't be null");
    }
    ClassLoader current = null;
    for (final Class<?> clazz : classes) {
      final ClassLoader proposed = clazz.getClassLoader();
      if (proposed == null) {
        continue;
      }
      if (current == null) {
        current = proposed;
      } else if (proposed != current && isParent(current, proposed)) {
        current = proposed;
      }
    }
    return current == null ? ClassLoader.getSystemClassLoader() : current;
  }

  /**
   * Finds the appropriate {@code ClassLoader} to be used by the {@link ServiceLoader#load} call,
   * which by default would use the context {@code ClassLoader}, which can be null. The fallback is
   * as follows: context ClassLoader, class ClassLoader and finaly the system ClassLoader.
   */
  public static ClassLoader findClassLoader() {
    return findClassLoader(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Checks if current is a parent of proposed.
   *
   * @param current the potential parent.
   * @param proposed the potential child.
   * @return true if current is in proposed hierarchy.
   */
  private static boolean isParent(final ClassLoader current, final ClassLoader proposed) {
    final Collection<ClassLoader> visited = new ArrayList<>();
    ClassLoader it = proposed.getParent();
    while (it != null) {
      if (it == current) {
        return true;
      }
      if (visited.contains(it)) { // avoid loops
        return false;
      }
      visited.add(it);
      it = it.getParent();
    }
    return false;
  }
}
