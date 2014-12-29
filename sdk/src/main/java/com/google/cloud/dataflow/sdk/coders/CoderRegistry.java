/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.coders;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.reflect.TypeToken;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A CoderRegistry allows registering the default Coder to use for a Java class,
 * and looking up and instantiating the default Coder for a Java type.
 *
 * <p> {@code CoderRegistry} uses the following mechanisms to determine a
 * default {@link Coder} for a Java class, in order of precedence:
 * <ul>
 *   <li> Registration: coders can be registered explicitly via
 *        {@link #registerCoder}.  Built-in types are registered via
 *        {@link #registerStandardCoders()}.
 *   <li> Annotations: {@link DefaultCoder} can be used to annotate a type with
 *        the default {@code Coder} type.
 *   <li> Inheritance: {@link Serializable} objects are given a default
 *        {@code Coder} of {@link SerializableCoder}.
 * </ul>
 */
public class CoderRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(CoderRegistry.class);

  /** A factory for default Coders for values of a particular class. */
  public abstract static class CoderFactory {
    /**
     * Returns the default Coder to use for values of a particular type,
     * given the Coders for each of the type's generic parameter types.
     * May return null if no default Coder can be created.
     */
    public abstract Coder<?> create(
        List<? extends Coder<?>> typeArgumentCoders);

    /**
     * Returns a list of objects contained in {@code value}, one per
     * type argument, or {@code null} if none can be determined.
     */
    public abstract List<Object> getInstanceComponents(Object value);
  }

  /** A factory that always returns the coder with which it is instantiated. */
  public class ConstantCoderFactory extends CoderFactory {
    private Coder<?> coder;

    public ConstantCoderFactory(Coder<?> coder) {
      this.coder = coder;
    }

    @Override
    public Coder<?> create(List<? extends Coder<?>> typeArgumentCoders) {
      return this.coder;
    }

    @Override
    public List<Object> getInstanceComponents(Object value) {
      return Collections.emptyList();
    }
  }

  public CoderRegistry() {}

  /**
   * Registers standard Coders with this CoderRegistry.
   */
  public void registerStandardCoders() {
    registerCoder(Double.class, DoubleCoder.class);
    registerCoder(Instant.class, InstantCoder.class);
    registerCoder(Integer.class, VarIntCoder.class);
    registerCoder(Iterable.class, IterableCoder.class);
    registerCoder(KV.class, KvCoder.class);
    registerCoder(List.class, ListCoder.class);
    registerCoder(Long.class, VarLongCoder.class);
    registerCoder(String.class, StringUtf8Coder.class);
    registerCoder(TableRow.class, TableRowJsonCoder.class);
    registerCoder(Void.class, VoidCoder.class);
    registerCoder(byte[].class, ByteArrayCoder.class);
    registerCoder(TimestampedValue.class, TimestampedValue.TimestampedValueCoder.class);
  }

  /**
   * Registers {@code coderClazz} as the default {@code Coder<T>}
   * class to handle encoding and decoding instances of {@code clazz}
   * of type {@code T}.
   *
   * <p> {@code coderClazz} should have a static factory method with the
   * following signature:
   *
   * <pre> {@code
   * public static Coder<T> of(Coder<X> argCoder1, Coder<Y> argCoder2, ...)
   * } </pre>
   *
   * <p> This method will be called to create instances of {@code Coder<T>}
   * for values of type {@code T}, passing Coders for each of the generic type
   * parameters of {@code T}.  If {@code T} takes no generic type parameters,
   * then the {@code of()} factory method should have no arguments.
   *
   * <p> If {@code T} is a parameterized type, then it should additionally
   * have a method with the following signature:
   *
   * <pre> {@code
   * public static List<Object> getInstanceComponents(T exampleValue);
   * } </pre>
   *
   * <p> This method will be called to decompose a value during the coder
   * inference process, to automatically choose coders for the components
   */
  public void registerCoder(Class<?> clazz,
                            Class<?> coderClazz) {
    int numTypeParameters = clazz.getTypeParameters().length;

    // Find the static factory method of coderClazz named 'of' with
    // the appropriate number of type parameters.

    Class<?>[] factoryMethodArgTypes = new Class<?>[numTypeParameters];
    Arrays.fill(factoryMethodArgTypes, Coder.class);

    Method factoryMethod;
    try {
      factoryMethod =
          coderClazz.getDeclaredMethod("of", factoryMethodArgTypes);
    } catch (NoSuchMethodException | SecurityException exn) {
      throw new IllegalArgumentException(
          "cannot register Coder " + coderClazz + ": "
          + "does not have an accessible method named 'of' with "
          + numTypeParameters + " arguments of Coder type",
          exn);
    }
    if (!Modifier.isStatic(factoryMethod.getModifiers())) {
      throw new IllegalArgumentException(
          "cannot register Coder " + coderClazz + ": "
          + "method named 'of' with " + numTypeParameters
          + " arguments of Coder type is not static");
    }
    if (!coderClazz.isAssignableFrom(factoryMethod.getReturnType())) {
      throw new IllegalArgumentException(
          "cannot register Coder " + coderClazz + ": "
          + "method named 'of' with " + numTypeParameters
          + " arguments of Coder type does not return a " + coderClazz);
    }
    try {
      if (!factoryMethod.isAccessible()) {
        factoryMethod.setAccessible(true);
      }
    } catch (SecurityException exn) {
      throw new IllegalArgumentException(
          "cannot register Coder " + coderClazz + ": "
          + "method named 'of' with " + numTypeParameters
          + " arguments of Coder type is not accessible",
          exn);
    }

    // Find the static method to decompose values when inferring a coder,
    // if there are type parameters for which we also need an example
    // value
    Method getComponentsMethod = null;
    if (clazz.getTypeParameters().length > 0) {
      try {
        getComponentsMethod = coderClazz.getDeclaredMethod(
            "getInstanceComponents",
            clazz);
      } catch (NoSuchMethodException | SecurityException exn) {
        LOG.warn("cannot find getInstanceComponents for class {}. This may limit the ability to"
            + " infer a Coder for values of this type.", coderClazz, exn);
      }
    }

    registerCoder(clazz, defaultCoderFactory(coderClazz, factoryMethod, getComponentsMethod));
  }

  public void registerCoder(Class<?> rawClazz,
                            CoderFactory coderFactory) {
    if (coderFactoryMap.put(rawClazz, coderFactory) != null) {
      throw new IllegalArgumentException(
          "cannot register multiple default Coder factories for " + rawClazz);
    }
  }

  public void registerCoder(Class<?> rawClazz, Coder<?> coder) {
    CoderFactory factory = new ConstantCoderFactory(coder);
    registerCoder(rawClazz, factory);
  }

  /**
   * Returns the Coder to use by default for values of the given type,
   * or null if there is no default Coder.
   */
  public <T> Coder<T> getDefaultCoder(TypeToken<T> typeToken) {
    return getDefaultCoder(typeToken, Collections.<Type, Coder<?>>emptyMap());
  }

  /**
   * Returns the Coder to use by default for values of the given type,
   * where the given context type uses the given context coder,
   * or null if there is no default Coder.
   */
  public <I, O> Coder<O> getDefaultCoder(TypeToken<O> typeToken,
                                         TypeToken<I> contextTypeToken,
                                         Coder<I> contextCoder) {
    return getDefaultCoder(typeToken,
                           createTypeBindings(contextTypeToken, contextCoder));
  }

  /**
   * Returns the Coder to use on elements produced by this function, given
   * the coder used for its input elements.
   */
  public <I, O> Coder<O> getDefaultOutputCoder(
      SerializableFunction<I, O> fn, Coder<I> inputCoder) {
    return getDefaultCoder(
        fn.getClass(), SerializableFunction.class, inputCoder);
  }

  /**
   * Returns the Coder to use for the last type parameter specialization
   * of the subclass given Coders to use for all other type parameters
   * specializations (if any).
   */
  public <T, O> Coder<O> getDefaultCoder(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Coder<?>... knownCoders) {
    Coder<?>[] allCoders = new Coder<?>[knownCoders.length + 1];
    // Last entry intentionally left null.
    System.arraycopy(knownCoders, 0, allCoders, 0, knownCoders.length);
    allCoders = getDefaultCoders(subClass, baseClass, allCoders);
    @SuppressWarnings("unchecked") // trusted
    Coder<O> coder = (Coder<O>) allCoders[knownCoders.length];
    return coder;
  }

  /**
   * Returns the Coder to use for the specified type parameter specialization
   * of the subclass, given Coders to use for all other type parameters
   * (if any).
   */
  @SuppressWarnings("unchecked")
  public <T, O> Coder<O> getDefaultCoder(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Map<String, ? extends Coder<?>> knownCoders,
      String paramName) {
    // TODO: Don't infer unneeded params.
    return (Coder<O>) getDefaultCoders(subClass, baseClass, knownCoders)
        .get(paramName);
  }

  /**
   * Returns the Coder to use for the provided example value, if it can
   * be determined, otherwise returns {@code null}. If more than one
   * default coder matches, this will raise an exception.
   */
  public <T> Coder<T> getDefaultCoder(T exampleValue) {
    Class<?> clazz = exampleValue.getClass();

    if (clazz.getTypeParameters().length == 0) {
      // Trust that getDefaultCoder returns a valid
      // Coder<T> for non-generic clazz.
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) getDefaultCoder(clazz);
      return coder;
    } else {
      CoderFactory factory = getDefaultCoderFactory(clazz);
      if (factory == null) {
        return null;
      }

      List<Object> components = factory.getInstanceComponents(exampleValue);
      if (components == null) {
        return null;
      }

      // componentcoders = components.map(this.getDefaultCoder)
      List<Coder<?>> componentCoders = new ArrayList<>();
      for (Object component : components) {
        Coder<?> componentCoder = getDefaultCoder(component);
        if (componentCoder == null) {
          return null;
        } else {
          componentCoders.add(componentCoder);
        }
      }

      // Trust that factory.create maps from valid component coders
      // to a valid Coder<T>.
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) factory.create(componentCoders);
      return coder;
    }
  }


  /**
   * Returns a Map from each of baseClass's type parameters to the Coder to
   * use by default for it, in the context of subClass's specialization of
   * baseClass.
   *
   * <P> For example, if baseClass is Map.class and subClass extends
   * {@code Map<String, Integer>} then this will return the registered Coders
   * to use for String and Integer as a {"K": stringCoder, "V": intCoder} Map.
   * The knownCoders parameter can be used to provide known coders for any of
   * the parameters which will be used to infer the others.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders a map corresponding to the set of known coders indexed
   *        by parameter name
   */
  public <T> Map<String, Coder<?>> getDefaultCoders(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Map<String, ? extends Coder<?>> knownCoders) {
    TypeVariable<Class<T>>[] typeParams = baseClass.getTypeParameters();
    Coder<?>[] knownCodersArray = new Coder<?>[typeParams.length];
    for (int i = 0; i < typeParams.length; i++) {
      knownCodersArray[i] = knownCoders.get(typeParams[i].getName());
    }
    Coder<?>[] resultArray = getDefaultCoders(
      subClass, baseClass, knownCodersArray);
    Map<String, Coder<?>> result = new HashMap<>();
    for (int i = 0; i < typeParams.length; i++) {
      result.put(typeParams[i].getName(), resultArray[i]);
    }
    return result;
  }

  /**
   * Returns an array listing, for each of baseClass's type parameters, the
   * Coder to use by default for it, in the context of subClass's specialization
   * of baseClass.
   *
   * <P> For example, if baseClass is Map.class and subClass extends
   * {@code Map<String, Integer>} then this will return the registered Coders
   * to use for String and Integer in that order.  The knownCoders parameter
   * can be used to provide known coders for any of the parameters which will
   * be used to infer the others.
   *
   * <P> If a type cannot be inferred, null is returned.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders an array corresponding to the set of base class
   *        type parameters.  Each entry is can be either a Coder (in which
   *        case it will be used for inference) or null (in which case it
   *        will be inferred).  May be null to indicate the entire set of
   *        parameters should be inferred.
   * @throws IllegalArgumentException if baseClass doesn't have type parameters
   *         or if the length of knownCoders is not equal to the number of type
   *         parameters
   */
  public <T> Coder<?>[] getDefaultCoders(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Coder<?>[] knownCoders) {
    Type type = TypeToken.of(subClass).getSupertype(baseClass).getType();
    if (!(type instanceof ParameterizedType)) {
      throw new IllegalArgumentException(type + " is not a ParameterizedType");
    }
    ParameterizedType parameterizedType = (ParameterizedType) type;
    Type[] typeArgs = parameterizedType.getActualTypeArguments();
    if (knownCoders == null) {
      knownCoders = new Coder<?>[typeArgs.length];
    } else if (typeArgs.length != knownCoders.length) {
      throw new IllegalArgumentException(
          "Class " + baseClass + " has " + typeArgs.length + " parameters, "
          + "but " + knownCoders.length + " coders are requested.");
    }
    Map<Type, Coder<?>> context = new HashMap<>();
    for (int i = 0; i < knownCoders.length; i++) {
      if (knownCoders[i] != null) {
        if (!isCompatible(knownCoders[i], typeArgs[i])) {
          throw new IllegalArgumentException(
              "Cannot encode elements of type " + typeArgs[i]
                  + " with " + knownCoders[i]);
        }
        context.put(typeArgs[i], knownCoders[i]);
      }
    }
    Coder<?>[] result = new Coder<?>[typeArgs.length];
    for (int i = 0; i < knownCoders.length; i++) {
      if (knownCoders[i] != null) {
        result[i] = knownCoders[i];
      } else {
        result[i] = getDefaultCoder(typeArgs[i], context);
      }
    }
    return result;
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns whether the given coder can possibly encode elements
   * of the given type.
   */
  static boolean isCompatible(Coder<?> coder, Type type) {
    Type coderType =
        ((ParameterizedType)
            TypeToken.of(coder.getClass()).getSupertype(Coder.class).getType())
        .getActualTypeArguments()[0];
    if (type instanceof TypeVariable) {
      return true; // Can't rule it out.
    }
    Class<?> coderClass = TypeToken.of(coderType).getRawType();
    if (!coderClass.isAssignableFrom(TypeToken.of(type).getRawType())) {
      return false;
    }
    if (coderType instanceof ParameterizedType
        && !isNullOrEmpty(coder.getCoderArguments())) {
      @SuppressWarnings("unchecked")
      Type[] typeArguments =
          ((ParameterizedType)
           TypeToken.of(type).getSupertype((Class) coderClass).getType())
          .getActualTypeArguments();
      List<? extends Coder<?>> typeArgumentCoders = coder.getCoderArguments();
      assert typeArguments.length == typeArgumentCoders.size();
      for (int i = 0; i < typeArguments.length; i++) {
        if (!isCompatible(
                typeArgumentCoders.get(i),
                TypeToken.of(type).resolveType(typeArguments[i]).getType())) {
          return false;
        }
      }
    }
    return true; // For all we can tell.
  }

  private static boolean isNullOrEmpty(Collection<?> c) {
    return c == null || c.size() == 0;
  }

  /**
   * The map of classes to the CoderFactories to use to create their
   * default Coders.
   */
  Map<Class<?>, CoderFactory> coderFactoryMap = new HashMap<>();

  /**
   * Returns a CoderFactory that invokes the given static factory method
   * to create the Coder.
   */
  static CoderFactory defaultCoderFactory(
      final Class<?> coderClazz,
      final Method coderFactoryMethod,
      final Method getComponentsMethod) {

    return new CoderFactory() {
      @Override
      public Coder<?> create(List<? extends Coder<?>> typeArgumentCoders) {
        try {
          return (Coder) coderFactoryMethod.invoke(
              null /* static */, typeArgumentCoders.toArray());
        } catch (IllegalAccessException |
                 IllegalArgumentException |
                 InvocationTargetException |
                 NullPointerException |
                 ExceptionInInitializerError exn) {
          throw new IllegalStateException(
              "error when invoking Coder factory method " + coderFactoryMethod,
              exn);
        }
      }

      @Override
      public List<Object> getInstanceComponents(Object value) {
        if (getComponentsMethod == null) {
          throw new IllegalStateException(
              "no suitable static getInstanceComponents method available for "
              + "Coder " + coderClazz);
        }

        try {
          @SuppressWarnings("unchecked")
          List<Object> result = (List<Object>) (getComponentsMethod.invoke(
              null /* static */, value));
          return result;
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
    };
  }

  static CoderFactory defaultCoderFactory(Class<?> coderClazz, final Method coderFactoryMethod) {
    return defaultCoderFactory(coderClazz, coderFactoryMethod, null);
  }

  /**
   * Returns the CoderFactory to use to create default Coders for
   * instances of the given class, or null if there is no default
   * CoderFactory registered.
   */
  CoderFactory getDefaultCoderFactory(Class<?> clazz) {
    CoderFactory coderFactory = coderFactoryMap.get(clazz);
    if (coderFactory == null) {
      LOG.debug("No Coder registered for {}", clazz);
    }
    return coderFactory;
  }

  /**
   * Returns the Coder to use by default for values of the given type,
   * in a context where the given types use the given coders,
   * or null if there is no default Coder.
   */
  <T> Coder<T> getDefaultCoder(TypeToken<T> typeToken,
                               Map<Type, Coder<?>> typeCoderBindings) {
    Coder<?> defaultCoder = getDefaultCoder(typeToken.getType(),
                                            typeCoderBindings);
    LOG.debug("Default Coder for {}: {}", typeToken, defaultCoder);
    @SuppressWarnings("unchecked")
    Coder<T> result = (Coder<T>) defaultCoder;
    return result;
  }

  /**
   * Returns the Coder to use by default for values of the given type,
   * in a context where the given types use the given coders,
   * or null if there is no default Coder.
   */
  Coder<?> getDefaultCoder(Type type, Map<Type, Coder<?>> typeCoderBindings) {
    Coder<?> coder = typeCoderBindings.get(type);
    if (coder != null) {
      return coder;
    }
    if (type instanceof Class<?>) {
      return getDefaultCoder((Class) type);
    } else if (type instanceof ParameterizedType) {
      return this.getDefaultCoder((ParameterizedType) type,
                                     typeCoderBindings);
    } else if (type instanceof TypeVariable
        || type instanceof WildcardType) {
      // No default coder for an unknown generic type.
      LOG.debug("No Coder for unknown generic type {}", type);
      return null;
    } else {
      throw new RuntimeException(
          "internal error: unexpected kind of Type: " + type);
    }
  }

  /**
   * Returns the Coder to use by default for values of the given
   * class, or null if there is no default Coder.
   */
  Coder<?> getDefaultCoder(Class<?> clazz) {
    CoderFactory coderFactory = getDefaultCoderFactory(clazz);
    if (coderFactory != null) {
      LOG.debug("Default Coder for {} found by factory", clazz);
      return coderFactory.create(Collections.<Coder<?>>emptyList());
    }

    DefaultCoder defaultAnnotation = clazz.getAnnotation(
        DefaultCoder.class);
    if (defaultAnnotation != null) {
      LOG.debug("Default Coder for {} found by DefaultCoder annotation", clazz);
      return InstanceBuilder.ofType(Coder.class)
          .fromClass(defaultAnnotation.value())
          .fromFactoryMethod("of")
          .withArg(Class.class, clazz)
          .build();
    }

    // Interface-based defaults.
    if (Serializable.class.isAssignableFrom(clazz)) {
      @SuppressWarnings("unchecked")
      Class<? extends Serializable> serializableClazz =
          (Class<? extends Serializable>) clazz;
      LOG.debug("Default Coder for {}: SerializableCoder", serializableClazz);
      return SerializableCoder.of(serializableClazz);
    }

    LOG.debug("No default Coder for {}", clazz);
    return null;
  }

  /**
   * Returns the Coder to use by default for values of the given
   * parameterized type, in a context where the given types use the
   * given coders, or null if there is no default Coder.
   */
  Coder<?> getDefaultCoder(
      ParameterizedType type,
      Map<Type, Coder<?>> typeCoderBindings) {
    Class<?> rawClazz = (Class) type.getRawType();
    CoderFactory coderFactory = getDefaultCoderFactory(rawClazz);
    if (coderFactory == null) {
      return null;
    }
    List<Coder<?>> typeArgumentCoders = new ArrayList<>();
    for (Type typeArgument : type.getActualTypeArguments()) {
      Coder<?> typeArgumentCoder = getDefaultCoder(typeArgument,
                                                   typeCoderBindings);
      if (typeArgumentCoder == null) {
        return null;
      }
      typeArgumentCoders.add(typeArgumentCoder);
    }
    return coderFactory.create(typeArgumentCoders);
  }

  /**
   * Returns a Map where each of the type variables embedded in the
   * given type are mapped to the corresponding Coders in the given
   * coder.
   */
  Map<Type, Coder<?>> createTypeBindings(TypeToken<?> typeToken,
                                         Coder<?> coder) {
    Map<Type, Coder<?>> typeCoderBindings = new HashMap<>();
    fillTypeBindings(typeToken.getType(), coder, typeCoderBindings);
    return typeCoderBindings;
  }

  /**
   * Adds to the given map bindings from each of the type variables
   * embedded in the given type to the corresponding Coders in the
   * given coder.
   */
  void fillTypeBindings(Type type,
                        Coder<?> coder,
                        Map<Type, Coder<?>> typeCoderBindings) {
    if (type instanceof TypeVariable) {
      LOG.debug("Binding type {} to Coder {}", type, coder);
      typeCoderBindings.put(type, coder);
    } else if (type instanceof ParameterizedType) {
      fillTypeBindings((ParameterizedType) type,
                       coder,
                       typeCoderBindings);
    }
  }

  /**
   * Adds to the given map bindings from each of the type variables
   * embedded in the given parameterized type to the corresponding
   * Coders in the given coder.
   */
  void fillTypeBindings(ParameterizedType type,
                        Coder<?> coder,
                        Map<Type, Coder<?>> typeCoderBindings) {
    Type[] typeArguments = type.getActualTypeArguments();
    List<? extends Coder<?>> coderArguments = coder.getCoderArguments();
    if (coderArguments == null
        || typeArguments.length != coderArguments.size()) {
      return;
    }
    for (int i = 0; i < typeArguments.length; i++) {
      fillTypeBindings(typeArguments[i],
                       coderArguments.get(i),
                       typeCoderBindings);
    }
  }
}
