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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.Invokable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.Parameter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.TypeResolver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.TypeToken;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A description of a Java type, including actual generic parameters where possible.
 *
 * <p>To prevent losing actual type arguments due to erasure, create an anonymous subclass with
 * concrete types:
 *
 * <pre>{@code
 * TypeDescriptor<List<String>> typeDescriptor = new TypeDescriptor<List<String>>() {};
 * }</pre>
 *
 * <p>If the above were not an anonymous subclass, the type {@code List<String>} would be erased and
 * unavailable at run time.
 *
 * @param <T> the type represented by this {@link TypeDescriptor}
 */
public abstract class TypeDescriptor<T> implements Serializable {

  // This class is just a wrapper for TypeToken
  private final TypeToken<T> token;

  /**
   * Creates a {@link TypeDescriptor} wrapping the provided token. This constructor is private so
   * Guava types do not leak.
   */
  private TypeDescriptor(TypeToken<T> token) {
    this.token = token;
  }

  /**
   * Creates a {@link TypeDescriptor} representing the type parameter {@code T}. To use this
   * constructor properly, the type parameter must be a concrete type, for example {@code new
   * TypeDescriptor<List<String>>(){}}.
   */
  protected TypeDescriptor() {
    token = new TypeToken<T>(getClass()) {};
  }

  /**
   * Creates a {@link TypeDescriptor} representing the type parameter {@code T}, which should
   * resolve to a concrete type in the context of the class {@code clazz}.
   *
   * <p>Unlike {@link TypeDescriptor#TypeDescriptor(Class)} this will also use context's of the
   * enclosing instances while attempting to resolve the type. This means that the types of any
   * classes instantiated in the concrete instance should be resolvable.
   */
  protected TypeDescriptor(Object instance) {
    TypeToken<?> unresolvedToken = new TypeToken<T>(getClass()) {};

    // While we haven't fully resolved the parameters, refine it using the captured
    // enclosing instance of the object.
    unresolvedToken = TypeToken.of(instance.getClass()).resolveType(unresolvedToken.getType());

    if (hasUnresolvedParameters(unresolvedToken.getType())) {
      for (Field field : instance.getClass().getDeclaredFields()) {
        Object fieldInstance = getEnclosingInstance(field, instance);
        if (fieldInstance != null) {
          unresolvedToken =
              TypeToken.of(fieldInstance.getClass()).resolveType(unresolvedToken.getType());
          if (!hasUnresolvedParameters(unresolvedToken.getType())) {
            break;
          }
        }
      }
    }

    // Once we've either fully resolved the parameters or exhausted enclosing instances, we have
    // the best approximation to the token we can get.
    @SuppressWarnings("unchecked")
    TypeToken<T> typedToken = (TypeToken<T>) unresolvedToken;
    token = typedToken;
  }

  private static boolean hasUnresolvedParameters(Type type) {
    if (type instanceof TypeVariable) {
      return true;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType param = (ParameterizedType) type;
      for (Type arg : param.getActualTypeArguments()) {
        if (hasUnresolvedParameters(arg)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the enclosing instance if the field is synthetic and it is able to access it, or
   * {@literal null} if not.
   */
  private static @Nullable Object getEnclosingInstance(Field field, Object instance) {
    if (!field.isSynthetic()) {
      return null;
    }

    boolean accessible = field.isAccessible();
    try {
      field.setAccessible(true);
      return field.get(instance);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      // If we fail to get the enclosing instance field, do nothing. In the worst case, we won't
      // refine the type based on information in this enclosing class -- that is consistent with
      // previous behavior and is still a correct answer that can be fixed by returning the correct
      // type descriptor.
      return null;
    } finally {
      field.setAccessible(accessible);
    }
  }

  /**
   * Creates a {@link TypeDescriptor} representing the type parameter {@code T}, which should
   * resolve to a concrete type in the context of the class {@code clazz}.
   */
  @SuppressWarnings("unchecked")
  protected TypeDescriptor(Class<?> clazz) {
    TypeToken<T> unresolvedToken = new TypeToken<T>(getClass()) {};
    token = (TypeToken<T>) TypeToken.of(clazz).resolveType(unresolvedToken.getType());
  }

  /** Returns a {@link TypeDescriptor} representing the given type. */
  public static <T> TypeDescriptor<T> of(Class<T> type) {
    return new SimpleTypeDescriptor<>(TypeToken.of(type));
  }

  /** Returns a {@link TypeDescriptor} representing the given type. */
  @SuppressWarnings("unchecked")
  public static TypeDescriptor<?> of(Type type) {
    return new SimpleTypeDescriptor<>((TypeToken<Object>) TypeToken.of(type));
  }

  /** Returns the {@link Type} represented by this {@link TypeDescriptor}. */
  public Type getType() {
    return token.getType();
  }

  /**
   * Returns the {@link Class} underlying the {@link Type} represented by this {@link
   * TypeDescriptor}.
   */
  public Class<? super T> getRawType() {
    return token.getRawType();
  }

  /** Returns the component type if this type is an array type, otherwise returns {@code null}. */
  public @Nullable TypeDescriptor<?> getComponentType() {
    @Nullable TypeToken<?> componentTypeToken = token.getComponentType();
    if (componentTypeToken == null) {
      return null;
    } else {
      return new SimpleTypeDescriptor<>(componentTypeToken);
    }
  }

  /** Returns the generic form of a supertype. */
  public final TypeDescriptor<? super T> getSupertype(Class<? super T> superclass) {
    return new SimpleTypeDescriptor<>(token.getSupertype(superclass));
  }

  /** Returns the generic form of a subtype. */
  public final TypeDescriptor<? extends T> getSubtype(Class<? extends T> subclass) {
    return new SimpleTypeDescriptor<>(token.getSubtype(subclass));
  }

  /** Returns true if this type is known to be an array type. */
  public final boolean isArray() {
    return token.isArray();
  }

  /**
   * Returns a {@link TypeVariable} for the named type parameter. Throws {@link
   * IllegalArgumentException} if a type variable by the requested type parameter is not found.
   *
   * <p>For example, {@code new TypeDescriptor<List>(){}.getTypeParameter("T")} returns a {@code
   * TypeVariable<? super List>} representing the formal type parameter {@code T}.
   *
   * <p>Do not mistake the type parameters (formal type argument list) with the actual type
   * arguments. For example, if a class {@code Foo} extends {@code List<String>}, it does not make
   * sense to ask for a type parameter, because {@code Foo} does not have any.
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

  /** Returns true if this type is assignable from the given type. */
  public final boolean isSupertypeOf(TypeDescriptor<?> source) {
    return token.isSupertypeOf(source.token);
  }

  /** Return true if this type is a subtype of the given type. */
  public final boolean isSubtypeOf(TypeDescriptor<?> parent) {
    return token.isSubtypeOf(parent.token);
  }

  /** Returns a list of argument types for the given method, which must be a part of the class. */
  public List<TypeDescriptor<?>> getArgumentTypes(Method method) {
    Invokable<?, ?> typedMethod = token.method(method);

    List<TypeDescriptor<?>> argTypes = Lists.newArrayList();
    for (Parameter parameter : typedMethod.getParameters()) {
      argTypes.add(new SimpleTypeDescriptor<>(parameter.getType()));
    }
    return argTypes;
  }

  /**
   * Returns a {@link TypeDescriptor} representing the given type, with type variables resolved
   * according to the specialization in this type.
   *
   * <p>For example, consider the following class:
   *
   * <pre>{@code
   * class MyList implements List<String> { ... }
   * }</pre>
   *
   * <p>The {@link TypeDescriptor} returned by
   *
   * <pre>{@code
   * TypeDescriptor.of(MyList.class)
   *     .resolveType(Mylist.class.getMethod("get", int.class).getGenericReturnType)
   * }</pre>
   *
   * will represent the type {@code String}.
   */
  public TypeDescriptor<?> resolveType(Type type) {
    return new SimpleTypeDescriptor<>(token.resolveType(type));
  }

  /**
   * Returns a set of {@link TypeDescriptor TypeDescriptor}, one for each superclass as well as each
   * interface implemented by this class.
   */
  @SuppressWarnings("rawtypes")
  public Iterable<TypeDescriptor> getTypes() {
    List<TypeDescriptor> interfaces = Lists.newArrayList();
    for (TypeToken<?> interfaceToken : token.getTypes()) {
      interfaces.add(new SimpleTypeDescriptor<>(interfaceToken));
    }
    return interfaces;
  }

  /** Returns a set of {@link TypeDescriptor}s, one for each interface implemented by this class. */
  @SuppressWarnings("rawtypes")
  public Iterable<TypeDescriptor> getInterfaces() {
    List<TypeDescriptor> interfaces = Lists.newArrayList();
    for (TypeToken<?> interfaceToken : token.getTypes().interfaces()) {
      interfaces.add(new SimpleTypeDescriptor<>(interfaceToken));
    }
    return interfaces;
  }

  /** Returns a set of {@link TypeDescriptor}s, one for each superclass (including this class). */
  @SuppressWarnings("rawtypes")
  public Iterable<TypeDescriptor> getClasses() {
    List<TypeDescriptor> classes = Lists.newArrayList();
    for (TypeToken<?> classToken : token.getTypes().classes()) {
      classes.add(new SimpleTypeDescriptor<>(classToken));
    }
    return classes;
  }

  /**
   * Returns a new {@code TypeDescriptor} where the type variable represented by {@code
   * typeParameter} are substituted by {@code type}. For example, it can be used to construct {@code
   * Map<K, V>} for any {@code K} and {@code V} type:
   *
   * <pre>{@code
   * static <K, V> TypeDescriptor<Map<K, V>> mapOf(
   *     TypeDescriptor<K> keyType, TypeDescriptor<V> valueType) {
   *   return new TypeDescriptor<Map<K, V>>() {}
   *       .where(new TypeParameter<K>() {}, keyType)
   *       .where(new TypeParameter<V>() {}, valueType);
   * }
   * }</pre>
   *
   * @param <X> The parameter type
   * @param typeParameter the parameter type variable
   * @param typeDescriptor the actual type to substitute
   */
  @SuppressWarnings("unchecked")
  public <X> TypeDescriptor<T> where(
      TypeParameter<X> typeParameter, TypeDescriptor<X> typeDescriptor) {
    return where(typeParameter.typeVariable, typeDescriptor.getType());
  }

  /**
   * A more general form of {@link #where(TypeParameter, TypeDescriptor)} that returns a new {@code
   * TypeDescriptor} by matching {@code formal} against {@code actual} to resolve type variables in
   * the current {@link TypeDescriptor}.
   */
  @SuppressWarnings("unchecked")
  public TypeDescriptor<T> where(Type formal, Type actual) {
    TypeResolver resolver = new TypeResolver().where(formal, actual);
    return (TypeDescriptor<T>) TypeDescriptor.of(resolver.resolveType(token.getType()));
  }

  /**
   * Returns whether this {@link TypeDescriptor} has any unresolved type parameters, as opposed to
   * being a concrete type.
   *
   * <p>For example:
   *
   * <pre>{@code
   * TypeDescriptor.of(new ArrayList<String>() {}.getClass()).hasUnresolvedTypeParameters()
   *   => false, because the anonymous class is instantiated with a concrete type
   *
   * class TestUtils {
   *   <T> ArrayList<T> createTypeErasedList() {
   *     return new ArrayList<T>() {};
   *   }
   * }
   *
   * TypeDescriptor.of(TestUtils.<String>createTypeErasedList().getClass())
   *   => true, because the type variable T got type-erased and the anonymous ArrayList class
   *   is instantiated with an unresolved type variable T.
   * }</pre>
   */
  public boolean hasUnresolvedParameters() {
    return hasUnresolvedParameters(getType());
  }

  @Override
  public String toString() {
    return token.toString();
  }

  /** Two type descriptor are equal if and only if they represent the same type. */
  @Override
  public boolean equals(@Nullable Object other) {
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
   * A non-abstract {@link TypeDescriptor} for construction directly from an existing {@link
   * TypeToken}.
   */
  private static final class SimpleTypeDescriptor<T> extends TypeDescriptor<T> {
    SimpleTypeDescriptor(TypeToken<T> typeToken) {
      super(typeToken);
    }
  }
}
