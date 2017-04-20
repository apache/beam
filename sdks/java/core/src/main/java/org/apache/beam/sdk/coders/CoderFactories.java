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
package org.apache.beam.sdk.coders;

import com.google.common.base.MoreObjects;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Static utility methods for creating and working with {@link Coder}s.
 */
public final class CoderFactories {
  private CoderFactories() { } // Static utility class

  /**
   * Creates a {@link CoderFactory} built from particular static methods of a class that
   * implements {@link Coder}.
   *
   * <p>The class must have the following static methods:
   *
   * <ul>
   * <li> {@code
   * public static Coder<T> of(Coder<X> argCoder1, Coder<Y> argCoder2, ...)
   * }
   * <li> {@code
   * public static List<Object> getInstanceComponents(T exampleValue);
   * }
   * </ul>
   *
   * <p>The {@code of(...)} method will be used to construct a
   * {@code Coder<T>} from component {@link Coder}s.
   * It must accept one {@link Coder} argument for each
   * generic type parameter of {@code T}. If {@code T} takes no generic
   * type parameters, then the {@code of()} factory method should take
   * no arguments.
   *
   * <p>The {@code getInstanceComponents} method will be used to
   * decompose a value during the {@link Coder} inference process,
   * to automatically choose coders for the components.
   *
   * <p>Note that the class {@code T} to be coded may be a
   * not-yet-specialized generic class.
   * For a generic class {@code MyClass<X>} and an actual type parameter
   * {@code Foo}, the {@link CoderFactoryFromStaticMethods} will
   * accept any {@code Coder<Foo>} and produce a {@code Coder<MyClass<Foo>>}.
   *
   * <p>For example, the {@link CoderFactory} returned by
   * {@code fromStaticMethods(ListCoder.class)}
   * will produce a {@code Coder<List<X>>} for any {@code Coder Coder<X>}.
   */
  public static <T> CoderFactory fromStaticMethods(Class<T> clazz) {
    return new CoderFactoryFromStaticMethods(clazz);
  }

  /**
   * Creates a {@link CoderFactory} that always returns the
   * given coder.
   *
   * <p>The {@code getInstanceComponents} method of this
   * {@link CoderFactory} always returns an empty list.
   */
  public static <T> CoderFactory forCoder(Coder<T> coder) {
    return new CoderFactoryForCoder<>(coder);
  }

  /**
   * See {@link #fromStaticMethods} for a detailed description
   * of the characteristics of this {@link CoderFactory}.
   */
  private static class CoderFactoryFromStaticMethods implements CoderFactory {

    @Override
    @SuppressWarnings("rawtypes")
    public Coder<?> create(List<? extends Coder<?>> componentCoders) {
      try {
        return (Coder) factoryMethod.invoke(
            null /* static */, componentCoders.toArray());
      } catch (IllegalAccessException
           | IllegalArgumentException
           | InvocationTargetException
           | NullPointerException
           | ExceptionInInitializerError exn) {
        throw new IllegalStateException(
            "error when invoking Coder factory method " + factoryMethod,
            exn);
      }
    }

    @Override
    public List<Object> getInstanceComponents(Object value) {
      try {
        @SuppressWarnings("unchecked")
        List<Object> components =  (List<Object>) getComponentsMethod.invoke(
            null /* static */, value);
        return components;
      } catch (IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | NullPointerException
          | ExceptionInInitializerError exn) {
        throw new IllegalStateException(
            "error when invoking Coder getComponents method " + getComponentsMethod,
            exn);
      }
    }

    ////////////////////////////////////////////////////////////////////////////////

    // Method to create a coder given component coders
    // For a Coder class of kind * -> * -> ... n times ... -> *
    // this has type Coder<?> -> Coder<?> -> ... n times ... -> Coder<T>
    private final Method factoryMethod;

    // Method to decompose a value of type T into its parts.
    // For a Coder class of kind * -> * -> ... n times ... -> *
    // this has type T -> List<Object>
    // where the list has n elements.
    private final Method getComponentsMethod;

    /**
     * Returns a CoderFactory that invokes the given static factory method
     * to create the Coder.
     */
    private CoderFactoryFromStaticMethods(Class<?> coderClazz) {
      this.factoryMethod = getFactoryMethod(coderClazz);
      this.getComponentsMethod = getInstanceComponentsMethod(coderClazz);
    }

    /**
     * Returns the static {@code of} constructor method on {@code coderClazz}
     * if it exists. It is assumed to have one {@link Coder} parameter for
     * each type parameter of {@code coderClazz}.
     */
    private Method getFactoryMethod(Class<?> coderClazz) {
      Method factoryMethodCandidate;

      // Find the static factory method of coderClazz named 'of' with
      // the appropriate number of type parameters.
      int numTypeParameters = coderClazz.getTypeParameters().length;
      Class<?>[] factoryMethodArgTypes = new Class<?>[numTypeParameters];
      Arrays.fill(factoryMethodArgTypes, Coder.class);
      try {
        factoryMethodCandidate =
            coderClazz.getDeclaredMethod("of", factoryMethodArgTypes);
      } catch (NoSuchMethodException | SecurityException exn) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "does not have an accessible method named 'of' with "
            + numTypeParameters + " arguments of Coder type",
            exn);
      }
      if (!Modifier.isStatic(factoryMethodCandidate.getModifiers())) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeParameters
            + " arguments of Coder type is not static");
      }
      if (!coderClazz.isAssignableFrom(factoryMethodCandidate.getReturnType())) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeParameters
            + " arguments of Coder type does not return a " + coderClazz);
      }
      try {
        if (!factoryMethodCandidate.isAccessible()) {
          factoryMethodCandidate.setAccessible(true);
        }
      } catch (SecurityException exn) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeParameters
            + " arguments of Coder type is not accessible",
            exn);
      }

      return factoryMethodCandidate;
    }

    /**
     * Finds the static method on {@code coderType} to use
     * to decompose a value of type {@code T} into components,
     * each corresponding to an argument of the {@code of}
     * method.
     */
    private <T> Method getInstanceComponentsMethod(Class<?> coderClazz) {
      TypeDescriptor<?> coderType = TypeDescriptor.of(coderClazz);
      TypeDescriptor<T> argumentType = getCodedType(coderType);

      // getInstanceComponents may be implemented in a superclass,
      // so we search them all for an applicable method. We do not
      // try to be clever about finding the best overload. It may
      // be in a generic superclass, erased to accept an Object.
      // However, subtypes are listed before supertypes (it is a
      // topological ordering) so probably the best one will be chosen
      // if there are more than one (which should be rare)
      for (TypeDescriptor<?> supertype : coderType.getClasses()) {
        for (Method method : supertype.getRawType().getDeclaredMethods()) {
          if (method.getName().equals("getInstanceComponents")) {
            TypeDescriptor<?> formalArgumentType = supertype.getArgumentTypes(method).get(0);
            if (formalArgumentType.getRawType().isAssignableFrom(argumentType.getRawType())) {
              return method;
            }
          }
        }
      }

      throw new IllegalArgumentException(
          "cannot create a CoderFactory from " + coderType + ": "
          + "does not have an accessible method "
          + "'getInstanceComponents'");
    }

    /**
     * If {@code coderType} is a subclass of {@link Coder} for a specific
     * type {@code T}, returns {@code T.class}. Otherwise, raises IllegalArgumentException.
     */
    private <T> TypeDescriptor<T> getCodedType(TypeDescriptor<?> coderType) {
      for (TypeDescriptor<?> ifaceType : coderType.getInterfaces()) {
        if (ifaceType.getRawType().equals(Coder.class)) {
          ParameterizedType coderIface = (ParameterizedType) ifaceType.getType();
          @SuppressWarnings("unchecked")
          TypeDescriptor<T> token =
              (TypeDescriptor<T>) TypeDescriptor.of(coderIface.getActualTypeArguments()[0]);
          return token;
        }
      }
      throw new IllegalArgumentException(
          "cannot build CoderFactory from class " + coderType
          + ": does not implement Coder<T> for any T.");
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("factoryMethod", factoryMethod)
          .add("getComponentsMethod", getComponentsMethod)
          .toString();
    }
  }

  /**
   * See {@link #forCoder} for a detailed description of this
   * {@link CoderFactory}.
   */
  private static class CoderFactoryForCoder<T> implements CoderFactory {
    private final Coder<T> coder;

    public CoderFactoryForCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public Coder<?> create(List<? extends Coder<?>> componentCoders) {
      return this.coder;
    }

    @Override
    public List<Object> getInstanceComponents(Object value) {
      return Collections.emptyList();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("coder", coder)
          .toString();
    }
  }
}
