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

package com.google.cloud.dataflow.sdk.coders;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
 * A {@code CoderRegistry} allows registering the
 * default {@link Coder} to use for a Java class,
 * and looking up and instantiating the default
 * {@link Coder} for a Java type.
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
public class CoderRegistry implements CoderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CoderRegistry.class);

  public CoderRegistry() {
    setFallbackCoderProvider(SerializableCoder.PROVIDER);
  }

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
    registerCoder(Map.class, MapCoder.class);
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
    registerCoder(clazz, CoderFactories.fromStaticMethods(coderClazz));
  }

  public void registerCoder(Class<?> rawClazz,
                            CoderFactory coderFactory) {
    if (coderFactoryMap.put(rawClazz, coderFactory) != null) {
      throw new IllegalArgumentException(
          "Cannot register multiple default Coder factories for " + rawClazz);
    }
  }

  public void registerCoder(Class<?> rawClazz, Coder<?> coder) {
    CoderFactory factory = CoderFactories.forCoder(coder);
    registerCoder(rawClazz, factory);
  }

  /**
   * Returns the Coder to use by default for values of the given type.
   *
   * @throws CannotProvideCoderException if there is no default Coder.
   */
  public <T> Coder<T> getDefaultCoder(TypeDescriptor<T> typeDescriptor)
      throws CannotProvideCoderException {
    return getDefaultCoder(typeDescriptor, Collections.<Type, Coder<?>>emptyMap());
  }

  /**
   * See {@link #getDefaultCoder(TypeDescriptor)}.
   */
  @Override
  public <T> Coder<T> getCoder(TypeDescriptor<T> typeDescriptor)
      throws CannotProvideCoderException {
    return getDefaultCoder(typeDescriptor);
  }

  /**
   * Returns the Coder to use by default for values of the given type,
   * where the given context type uses the given context coder.
   *
   * @throws CannotProvideCoderException if there is no default Coder.
   */
  public <InputT, OutputT> Coder<OutputT> getDefaultCoder(
      TypeDescriptor<OutputT> typeDescriptor,
      TypeDescriptor<InputT> contextTypeDescriptor,
      Coder<InputT> contextCoder)
      throws CannotProvideCoderException {
    return getDefaultCoder(typeDescriptor,
                           getTypeToCoderBindings(contextTypeDescriptor.getType(), contextCoder));
  }

  /**
   * Returns the Coder to use on elements produced by this function, given
   * the coder used for its input elements.
   */
  public <InputT, OutputT> Coder<OutputT> getDefaultOutputCoder(
      SerializableFunction<InputT, OutputT> fn, Coder<InputT> inputCoder)
      throws CannotProvideCoderException {
    return getDefaultCoder(
        fn.getClass(), SerializableFunction.class, inputCoder);
  }

  /**
   * Returns the Coder to use for the last type parameter specialization
   * of the subclass given Coders to use for all other type parameters
   * specializations (if any).
   *
   * @throws CannotProvideCoderException if there is no default Coder.
   */
  public <T, OutputT> Coder<OutputT> getDefaultCoder(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Coder<?>... knownCoders) throws CannotProvideCoderException {
    Coder<?>[] allCoders = new Coder<?>[knownCoders.length + 1];
    // Last entry intentionally left null.
    System.arraycopy(knownCoders, 0, allCoders, 0, knownCoders.length);
    allCoders = getDefaultCoders(subClass, baseClass, allCoders);
    @SuppressWarnings("unchecked") // trusted
    Coder<OutputT> coder = (Coder<OutputT>) allCoders[knownCoders.length];
    return coder;
  }

  /**
   * Returns the Coder to use for the specified type parameter specialization
   * of the subclass, given Coders to use for all other type parameters
   * (if any).
   *
   * @throws CannotProvideCoderException if there is no default Coder.
   */
  public <T, OutputT> Coder<OutputT> getDefaultCoder(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Map<Type, ? extends Coder<?>> knownCoders,
      TypeVariable<?> param)
      throws CannotProvideCoderException {

    Map<Type, Coder<?>> inferredCoders = getDefaultCoders(subClass, baseClass, knownCoders);

    @SuppressWarnings("unchecked")
    Coder<OutputT> paramCoderOrNull = (Coder<OutputT>) inferredCoders.get(param);
    if (paramCoderOrNull != null) {
      return paramCoderOrNull;
    } else {
      throw new CannotProvideCoderException(
          "Cannot infer coder for type parameter " + param.getName());
    }
  }

  /**
   * Returns the Coder to use for the provided example value, if it can
   * be determined.
   *
   * @throws CannotProvideCoderException if there is no default Coder or
   * more than one coder matches
   */
  public <T> Coder<T> getDefaultCoder(T exampleValue) throws CannotProvideCoderException {
    Class<?> clazz = exampleValue == null ? Void.class : exampleValue.getClass();

    if (clazz.getTypeParameters().length == 0) {
      // Trust that getDefaultCoder returns a valid
      // Coder<T> for non-generic clazz.
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) getDefaultCoder(clazz);
      return coder;
    } else {
      CoderFactory factory = getDefaultCoderFactory(clazz);

      List<Object> components = factory.getInstanceComponents(exampleValue);
      if (components == null) {
        throw new CannotProvideCoderException(
            "Cannot provide coder based on value with class "
            + clazz + ": The registered CoderFactory with class "
            + factory.getClass() + " failed to decompose the value, "
            + "which is required in order to provide coders for the components.");
      }

      // componentcoders = components.map(this.getDefaultCoder)
      List<Coder<?>> componentCoders = new ArrayList<>();
      for (Object component : components) {
        try {
          Coder<?> componentCoder = getDefaultCoder(component);
          componentCoders.add(componentCoder);
        } catch (CannotProvideCoderException exc) {
          throw new CannotProvideCoderException(
              "Cannot provide coder based on value with class " + clazz, exc);
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
   * <p> If no coder can be inferred for a particular type parameter,
   * then that type variable will be absent from the returned map.
   *
   * <p> For example, if baseClass is Map.class and subClass extends
   * {@code Map<String, Integer>} then this will return the registered Coders
   * to use for String and Integer as a {"K": stringCoder, "V": intCoder} Map.
   * The knownCoders parameter can be used to provide known coders for any of
   * the parameters that will be used to infer the others.
   *
   * <p> Note that inference is attempted for every type variable.
   * For a type {@code MyType<One, Two, Three>} inference will will be
   * attempted for all of {@code One}, {@code Two}, {@code Three},
   * even if the requester only wants a coder for {@code Two}.
   *
   * <p> For this reason, {@code getDefaultCoders} (plural) does not throw
   * an exception if a coder for a particular type variable cannot be
   * inferred. Instead, it is left absent from the map. It is the responsibility
   * of the caller (usually {@link #getDefaultCoder} to extract the
   * desired coder or throw a {@link CannotProvideCoderException} when appropriate.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders a map corresponding to the set of known coders indexed
   *        by parameter name
   */
  public <T> Map<Type, Coder<?>> getDefaultCoders(
      Class<? extends T> subClass,
      Class<T> baseClass,
      Map<Type, ? extends Coder<?>> knownCoders) {
    TypeVariable<Class<T>>[] typeParams = baseClass.getTypeParameters();
    Coder<?>[] knownCodersArray = new Coder<?>[typeParams.length];
    for (int i = 0; i < typeParams.length; i++) {
      knownCodersArray[i] = knownCoders.get(typeParams[i]);
    }
    Coder<?>[] resultArray = getDefaultCoders(
      subClass, baseClass, knownCodersArray);
    Map<Type, Coder<?>> result = new HashMap<>();
    for (int i = 0; i < typeParams.length; i++) {
      if (resultArray[i] != null) {
        result.put(typeParams[i], resultArray[i]);
      }
    }
    return result;
  }

  /**
   * Returns an array listing, for each of {@code baseClass}'s type parameters, the
   * Coder to use by default for it, in the context of {@code subClass}'s specialization
   * of {@code baseClass}.
   *
   * <p> If a coder cannot be inferred for a type variable, its slot in the
   * resulting array will be {@code null}.
   *
   * <p> For example, if {@code baseClass} is {@code Map.class} and {@code subClass}
   * extends {@code Map<String, Integer>} then this will return the registered Coders
   * to use for {@code String} and {@code Integer}, in that order.
   * The {@code knownCoders} parameter can be used to provide known coders
   * for any of the parameters that will be used to infer the others.
   *
   * <p> Note that inference is attempted for every type variable.
   * For a type {@code MyType<One, Two, Three>} inference will will be
   * attempted for all of {@code One}, {@code Two}, {@code Three},
   * even if the requester only wants a coder for {@code Two}.
   *
   * <p> For this reason {@code getDefaultCoders} (plural) does not throw
   * an exception if a coder for a particular type variable cannot be
   * inferred. Instead, it results in a {@code null} in the array.
   * It is the responsibility of the caller (usually {@link #getDefaultCoder}
   * to extract the desired coder or throw a {@link CannotProvideCoderException}
   * when appropriate.
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
    Type type = TypeDescriptor.of(subClass).getSupertype(baseClass).getType();
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
        context.putAll(getTypeToCoderBindings(typeArgs[i], knownCoders[i]));
      }
    }

    Coder<?>[] result = new Coder<?>[typeArgs.length];
    for (int i = 0; i < knownCoders.length; i++) {
      if (knownCoders[i] != null) {
        result[i] = knownCoders[i];
      } else {
        try {
          result[i] = getDefaultCoder(typeArgs[i], context);
        } catch (CannotProvideCoderException exc) {
          result[i] = null;
        }
      }
    }
    return result;
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns {@code true} if the given coder can possibly encode elements
   * of the given type.
   */
  static <T, CoderT extends Coder<T>, CandidateT> boolean
      isCompatible(CoderT coder, Type candidateType) {

    // Various representations of the coder's class
    @SuppressWarnings("unchecked")
    Class<CoderT> coderClass = (Class<CoderT>) coder.getClass();
    TypeDescriptor<CoderT> coderDescriptor = TypeDescriptor.of(coderClass);

    // Various representations of the actual coded type
    @SuppressWarnings("unchecked")
    TypeDescriptor<T> codedDescriptor = CoderUtils.getCodedType(coderDescriptor);
    @SuppressWarnings("unchecked")
    Class<T> codedClass = (Class<T>) codedDescriptor.getRawType();
    Type codedType = codedDescriptor.getType();

    // Various representations of the candidate type
    @SuppressWarnings("unchecked")
    TypeDescriptor<CandidateT> candidateDescriptor =
        (TypeDescriptor<CandidateT>) TypeDescriptor.<CandidateT>of(candidateType);
    @SuppressWarnings("unchecked")
    Class<CandidateT> candidateClass = (Class<CandidateT>) candidateDescriptor.getRawType();

    // If coder has type Coder<T> where the actual value of T is lost
    // to erasure, then we cannot rule it out.
    if (candidateType instanceof TypeVariable) {
      return true;
    }

    // If the raw types are not compatible, we can certainly rule out
    // coder compatibility
    if (!codedClass.isAssignableFrom(candidateClass)) {
      return false;
    }
    // we have established that this is a covariant upcast... though
    // coders are invariant, we are just checking one direction
    @SuppressWarnings("unchecked")
    TypeDescriptor<T> candidateOkDescriptor = (TypeDescriptor<T>) candidateDescriptor;

    // If the coded type is a parameterized type where any of the actual
    // type parameters are not compatible, then the whole thing is certainly not
    // compatible.
    if ((codedType instanceof ParameterizedType) && !isNullOrEmpty(coder.getCoderArguments())) {
      @SuppressWarnings("unchecked")
      Type[] typeArguments =
          ((ParameterizedType)
           candidateOkDescriptor.getSupertype(codedClass).getType())
          .getActualTypeArguments();
      List<? extends Coder<?>> typeArgumentCoders = coder.getCoderArguments();
      assert typeArguments.length == typeArgumentCoders.size();
      for (int i = 0; i < typeArguments.length; i++) {
        if (!isCompatible(
                typeArgumentCoders.get(i),
                candidateDescriptor.resolveType(typeArguments[i]).getType())) {
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
    * A provider of coders for types where no coder is registered.
    */
   private CoderProvider fallbackCoderProvider;

  /**
   * Returns the CoderFactory to use to create default Coders for
   * instances of the given class, or null if there is no default
   * CoderFactory registered.
   */
  CoderFactory getDefaultCoderFactory(Class<?> clazz) throws CannotProvideCoderException {
    CoderFactory coderFactoryOrNull = coderFactoryMap.get(clazz);
    if (coderFactoryOrNull != null) {
      return coderFactoryOrNull;
    } else {
      throw new CannotProvideCoderException(
          "Cannot provide coder based on value with class "
          + clazz + ": No CoderFactory has been registered for the class.");
    }
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given type,
   * in a context where the given types use the given coders.
   *
   * @throws CannotProvideCoderException if a coder cannot be provided
   */
  <T> Coder<T> getDefaultCoder(
      TypeDescriptor<T> typeDescriptor,
      Map<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {

    Coder<?> defaultCoder = getDefaultCoder(typeDescriptor.getType(),
                                            typeCoderBindings);
    LOG.debug("Default Coder for {}: {}", typeDescriptor, defaultCoder);
    @SuppressWarnings("unchecked")
    Coder<T> result = (Coder<T>) defaultCoder;
    return result;
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given type,
   * in a context where the given types use the given coders.
   *
   * @throws CannotProvideCoderException if a coder cannot be provided
   */
  Coder<?> getDefaultCoder(Type type, Map<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {
    Coder<?> coder = typeCoderBindings.get(type);
    if (coder != null) {
      return coder;
    }
    if (type instanceof Class<?>) {
      @SuppressWarnings("unchecked")
      Class<?> clazz = (Class<?>) type;
      return getDefaultCoder(clazz);
    } else if (type instanceof ParameterizedType) {
      return getDefaultCoder((ParameterizedType) type, typeCoderBindings);
    } else if (type instanceof TypeVariable
        || type instanceof WildcardType) {
      // No default coder for an unknown generic type.
      throw new CannotProvideCoderException(
          "Cannot provide a Coder for type variable "
          + type + " because the actual type is unknown due to erasure.");
    } else {
      throw new RuntimeException(
          "Internal error: unexpected kind of Type: " + type);
    }
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given
   * class.
   *
   * <ol>
   * <li>A {@link Coder} class registered explicitly via
   * a call to {@link #registerCoder},
   * <li>A {@link DefaultCoder} annotation on the class,
   * <li>This registry's fallback {@link CoderProvider}, which
   * may be able to generate a coder for an arbitrary class.
   * </ol>
   *
   * @throws CannotProvideCoderException if a coder cannot be provided
   */
  <T> Coder<T> getDefaultCoder(Class<T> clazz) throws CannotProvideCoderException {
    try {
      CoderFactory coderFactory = getDefaultCoderFactory(clazz);
      LOG.debug("Default Coder for {} found by factory", clazz);
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) coderFactory.create(Collections.<Coder<?>>emptyList());
      return coder;
    } catch (CannotProvideCoderException exc) {
      // try other ways of finding one
    }

    DefaultCoder defaultAnnotation = clazz.getAnnotation(
        DefaultCoder.class);
    if (defaultAnnotation != null) {
      LOG.debug("Default Coder for {} found by DefaultCoder annotation", clazz);
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) InstanceBuilder.ofType(Coder.class)
          .fromClass(defaultAnnotation.value())
          .fromFactoryMethod("of")
          .withArg(Class.class, clazz)
          .build();
      return coder;
    }

    if (getFallbackCoderProvider() != null) {
      try {
        return getFallbackCoderProvider().getCoder(TypeDescriptor.<T>of(clazz));
      } catch (CannotProvideCoderException exc) {
        throw new CannotProvideCoderException(
            "Cannot provide coder for class " + clazz + " because "
            + "it has no " + CoderFactory.class.getSimpleName() + " registered, "
            + "it has no " + DefaultCoder.class.getSimpleName() + " annotation, "
            + "and the fallback " + CoderProvider.class.getSimpleName()
            + " could not automatically create a Coder.",
            exc);
      }
    } else {
      throw new CannotProvideCoderException(
            "Cannot provide coder for class " + clazz + " because "
            + "it has no " + CoderFactory.class.getSimpleName() + " registered, "
            + "it has no " + DefaultCoder.class.getSimpleName() + " annotation, "
            + "and there is no fallback CoderProvider configured.");
    }
  }

  public void setFallbackCoderProvider(CoderProvider coderProvider) {
    fallbackCoderProvider = coderProvider;
  }

  public CoderProvider getFallbackCoderProvider() {
    return fallbackCoderProvider;
  }

  /**
   * Returns the Coder to use by default for values of the given
   * parameterized type, in a context where the given types use the
   * given coders.
   *
   * @throws CannotProvideCoderException if no coder can be provided
   */
  Coder<?> getDefaultCoder(
      ParameterizedType type,
      Map<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {
    Class<?> rawClazz = (Class<?>) type.getRawType();
    CoderFactory coderFactory = getDefaultCoderFactory(rawClazz);
    List<Coder<?>> typeArgumentCoders = new ArrayList<>();
    for (Type typeArgument : type.getActualTypeArguments()) {
      try {
        Coder<?> typeArgumentCoder = getDefaultCoder(typeArgument,
                                                     typeCoderBindings);
        typeArgumentCoders.add(typeArgumentCoder);
      } catch (CannotProvideCoderException exc) {
         throw new CannotProvideCoderException(
          "Cannot provide coder for parameterized type " + type,
          exc);
      }
    }
    return coderFactory.create(typeArgumentCoders);
  }

  /**
   * Returns an immutable {@code Map} from each of the type variables
   * embedded in the given type to the corresponding types
   * in the given coder.
   */
  private Map<Type, Coder<?>> getTypeToCoderBindings(Type type, Coder<?> coder) {
    if (type instanceof TypeVariable || type instanceof Class) {
      return ImmutableMap.<Type, Coder<?>>of(type, coder);
    } else if (type instanceof ParameterizedType) {
      return getTypeToCoderBindings((ParameterizedType) type, coder);
    } else {
      return ImmutableMap.of();
    }
  }

  /**
   * Returns an immutable {@code Map} from the type arguments of the
   * parameterized type to their corresponding coders, and so on recursively
   * for their type parameters.
   *
   * <p>This method is simply a specialization to break out the most
   * elaborate case of {@link #getTypeToCoderBindings(Type, Coder)}.
   */
  private Map<Type, Coder<?>> getTypeToCoderBindings(ParameterizedType type, Coder<?> coder) {
    List<Type> typeArguments = Arrays.asList(type.getActualTypeArguments());
    List<? extends Coder<?>> coderArguments = coder.getCoderArguments();

    if ((coderArguments == null) || (typeArguments.size() != coderArguments.size())) {
      return ImmutableMap.of();
    } else {
      Map<Type, Coder<?>> typeToCoder = Maps.newHashMap();

      typeToCoder.put(type, coder);

      for (int i = 0; i < typeArguments.size(); i++) {
        Type typeArgument = typeArguments.get(i);
        Coder<?> coderArgument = coderArguments.get(i);
        typeToCoder.putAll(getTypeToCoderBindings(typeArgument, coderArgument));
      }

      return ImmutableMap.<Type, Coder<?>>builder().putAll(typeToCoder).build();
    }

  }
}
