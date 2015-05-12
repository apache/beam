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

package com.google.cloud.dataflow.sdk.values;

import com.google.common.collect.Lists;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;

/**
 * A description of a Java type, including actual generic parameters where possible.
 *
 * <p> To prevent losing actual type arguments due to erasure, create an anonymous subclass
 * with concrete types:
 * <pre>
 * {@code
 * TypeDecriptor<List<String>> = new TypeDescriptor<List<String>>() {};
 * }
 * </pre>
 *
 * <p>If the above were not an anonymous subclass, the type {@code List<String>}
 * would be erased and unavailable at run time.
 *
 * @param <T> the type represented by this {@link TypeDescriptor}
 */
public abstract class TypeDescriptor<T> {

  // This class is just a wrapper for TypeToken
  private final TypeToken<T> token;

  /**
   * Creates a TypeDescriptor wrapping the provided token.
   * This constructor is private so Guava types do not leak.
   */
  private TypeDescriptor(TypeToken<T> token) {
    this.token = token;
  }

  /**
   * Creates a {@link TypeDescriptor} representing
   * the type parameter {@code T}. To use this constructor
   * properly, the type parameter must be a concrete type, for example
   * {@code new TypeDescriptor<List<String>>(){}}.
   */
  protected TypeDescriptor() {
    token = new TypeToken<T>(getClass()) {
      private static final long serialVersionUID = 0L;
    };
  }

  /**
   * Creates a {@link TypeDescriptor} representing the type parameter
   * {@code T}, which should resolve to a concrete type in the context
   * of the class {@code clazz}.
   */
  @SuppressWarnings("unchecked")
  protected TypeDescriptor(Class<?> clazz) {
    TypeToken<T> unresolvedToken = new TypeToken<T>(getClass()) {
      private static final long serialVersionUID = 0L;
    };
    token = (TypeToken<T>) TypeToken.of(clazz).resolveType(unresolvedToken.getType());
  }

  /**
   * Returns a {@link TypeDescriptor} representing the given type.
   */
  public static <T> TypeDescriptor<T> of(Class<T> type) {
    return new SimpleTypeDescriptor<>(TypeToken.<T>of(type));
  }

  /**
   * Returns a {@link TypeDescriptor} representing the given type.
   */
  @SuppressWarnings("unchecked")
  public static TypeDescriptor<?> of(Type type) {
    return new SimpleTypeDescriptor<>((TypeToken<Object>) TypeToken.of(type));
  }

  /**
   * Returns the {@code Type} represented by this {@link TypeDescriptor}.
   */
  public Type getType() {
    return token.getType();
  }

  /**
   * Returns the {@code Class} underlying the {@code Type} represented by
   * this {@link TypeDescriptor}.
   */
  public Class<? super T> getRawType() {
    return token.getRawType();
  }

  /**
   * Returns the component type if this type is an array type,
   * otherwise returns {@code null}.
   */
  public TypeDescriptor<?> getComponentType() {
    return new SimpleTypeDescriptor<>(token.getComponentType());
  }

  /**
   * Returns the generic form of a supertype.
   */
  public final TypeDescriptor<? super T> getSupertype(Class<? super T> superclass) {
    return new SimpleTypeDescriptor<>(token.getSupertype(superclass));
  }

  /**
   * Returns true if this type is known to be an array type.
   */
  public final boolean isArray() {
    return token.isArray();
  }

  /**
   * Returns a {@code TypeVariable} for the named type parameter. Throws
   * {@code IllegalArgumentException} if a type variable by the requested type parameter is not
   * found.
   *
   * <p>For example, {@code new TypeDescriptor<List>(){}.getTypeParameter("T")} returns a
   * {@code TypeVariable<? super List>} representing the formal type parameter {@code T}.
   *
   * <p>Do not mistake the type parameters (formal type argument list) with the actual
   * type arguments. For example, if a class {@code Foo} extends {@code List<String>}, it
   * does not make sense to ask for a type parameter, because {@code Foo} does not have any.
   */
  public final TypeVariable<Class<? super T>> getTypeParameter(String paramName) {
    // Cannot convert TypeVariable<Class<? super T>>[] to TypeVariable<Class<? super T>>[]
    // due to how they are used here, so the result of getTypeParameters() cannot be used
    // without upcast.
    Class<?> rawType = getRawType();
    for (TypeVariable<?> param : rawType.getTypeParameters()) {
      if (param.getName().equals(paramName)) {
        @SuppressWarnings("unchecked")
        TypeVariable<Class<? super T>> typedParam = (TypeVariable<Class<? super T>>) param;
        return typedParam;
      }
    }
     throw new IllegalArgumentException(
         "No type parameter named " + paramName + " found on " + getRawType());
  }

  /**
   * Returns true if this type is assignable from the given type.
   */
  public final boolean isSupertypeOf(TypeDescriptor<?> source) {
    return token.isAssignableFrom(source.token);
  }

  /**
   * Return true if this type is a subtype of the given type.
   */
  public final boolean isSubtypeOf(TypeDescriptor<?> parent) {
    return parent.token.isAssignableFrom(token);
  }

  /**
   * Returns a list of argument types for the given method, which must
   * be a part of the class.
   */
  public List<TypeDescriptor<?>> getArgumentTypes(Method method) {
    Invokable<?, ?> typedMethod = token.method(method);

    List<TypeDescriptor<?>> argTypes = Lists.newArrayList();
    for (Parameter parameter : typedMethod.getParameters()) {
      argTypes.add(new SimpleTypeDescriptor<>(parameter.getType()));
    }
    return argTypes;
  }

  /**
   * Returns a {@code TypeDescriptor} representing the given
   * type, with type variables resolved according to the specialization
   * in this type.
   *
   * <p>For example, consider the following class:
   * <pre>
   * {@code
   * class MyList implements List<String> { ... }
   * }
   * </pre>
   *
   * <p>The {@link TypeDescriptor} returned by
   * <pre>
   * {@code
   * TypeDescriptor.of(MyList.class)
   *     .resolveType(Mylist.class.getMethod("get", int.class).getGenericReturnType)
   * }
   * </pre>
   * will represent the type {@code String}.
   */
  public TypeDescriptor<?> resolveType(Type type) {
    return new SimpleTypeDescriptor<>(token.resolveType(type));
  }

  /**
   * Returns a set of {@link TypeDescriptor}s, one for each
   * interface implemented by this class.
   */
  @SuppressWarnings("rawtypes")
  public Iterable<TypeDescriptor> getInterfaces() {
    List<TypeDescriptor> interfaces = Lists.newArrayList();
    for (TypeToken<?> interfaceToken : token.getTypes().interfaces()) {
      interfaces.add(new SimpleTypeDescriptor<>(interfaceToken));
    }
    return interfaces;
  }

  /**
   * Returns a set of {@link TypeDescriptor}s, one for each
   * superclass (including this class).
   */
  @SuppressWarnings("rawtypes")
  public Iterable<TypeDescriptor> getClasses() {
    List<TypeDescriptor> classes = Lists.newArrayList();
    for (TypeToken<?> classToken : token.getTypes().classes()) {
      classes.add(new SimpleTypeDescriptor<>(classToken));
    }
    return classes;
  }

  @Override
  public String toString() {
    return token.toString();
  }

  /**
   * Two type descriptor are equal if and only if they
   * represent the same type.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TypeDescriptor)) {
      return false;
    } else {
      @SuppressWarnings("unchecked")
      TypeDescriptor<?> descriptor = (TypeDescriptor<?>) other;
      return token.equals(descriptor.token);
    }
  }

  @Override
  public int hashCode() {
    return token.hashCode();
  }

  /**
   * A non-abstract {@link TypeDescriptor} for construction directly from an existing
   * {@link TypeToken}.
   */
  private static final class SimpleTypeDescriptor<T> extends TypeDescriptor<T> {
    SimpleTypeDescriptor(TypeToken<T> typeToken) {
      super(typeToken);
    }
  }
}
