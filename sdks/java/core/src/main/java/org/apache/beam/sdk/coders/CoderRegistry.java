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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException.ReasonCode;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MetadataCoder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.common.ReflectHelpers.ObjectsClassComparator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSetMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.SetMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CoderRegistry} allows creating a {@link Coder} for a given Java {@link Class class} or
 * {@link TypeDescriptor type descriptor}.
 *
 * <p>Creation of the {@link Coder} is delegated to one of the many registered {@link CoderProvider
 * coder providers} based upon the registration order.
 *
 * <p>By default, the {@link CoderProvider coder provider} precedence order is as follows:
 *
 * <ul>
 *   <li>Coder providers registered programmatically with {@link
 *       CoderRegistry#registerCoderProvider(CoderProvider)}.
 *   <li>A default coder provider for common Java (Byte, Double, List, ...) and Apache Beam (KV,
 *       ...) types.
 *   <li>Coder providers registered automatically through a {@link CoderProviderRegistrar} using a
 *       {@link ServiceLoader}. Note that the {@link ServiceLoader} registration order is consistent
 *       but may change due to the addition or removal of libraries exposed to the application. This
 *       can impact the coder returned if multiple coder providers are capable of supplying a coder
 *       for the specified type.
 * </ul>
 *
 * <p>Note that if multiple {@link CoderProvider coder providers} can provide a {@link Coder} for a
 * given type, the precedence order above defines which {@link CoderProvider} is chosen.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoderRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(CoderRegistry.class);
  private static final List<CoderProvider> REGISTERED_CODER_FACTORIES;

  /** A {@link CoderProvider} for common Java SDK and Apache Beam SDK types. */
  private static class CommonTypes extends CoderProvider {
    private final Map<Class<?>, CoderProvider> commonTypesToCoderProviders;

    private CommonTypes() {
      ImmutableMap.Builder<Class<?>, CoderProvider> builder = ImmutableMap.builder();
      builder.put(
          Boolean.class, CoderProviders.fromStaticMethods(Boolean.class, BooleanCoder.class));
      builder.put(Byte.class, CoderProviders.fromStaticMethods(Byte.class, ByteCoder.class));
      builder.put(BitSet.class, CoderProviders.fromStaticMethods(BitSet.class, BitSetCoder.class));
      builder.put(Float.class, CoderProviders.fromStaticMethods(Float.class, FloatCoder.class));
      builder.put(Double.class, CoderProviders.fromStaticMethods(Double.class, DoubleCoder.class));
      builder.put(
          Instant.class, CoderProviders.fromStaticMethods(Instant.class, InstantCoder.class));
      builder.put(
          Integer.class, CoderProviders.fromStaticMethods(Integer.class, VarIntCoder.class));
      builder.put(
          Optional.class, CoderProviders.fromStaticMethods(Optional.class, OptionalCoder.class));
      builder.put(
          Iterable.class, CoderProviders.fromStaticMethods(Iterable.class, IterableCoder.class));
      builder.put(KV.class, CoderProviders.fromStaticMethods(KV.class, KvCoder.class));
      builder.put(List.class, CoderProviders.fromStaticMethods(List.class, ListCoder.class));
      builder.put(Long.class, CoderProviders.fromStaticMethods(Long.class, VarLongCoder.class));
      builder.put(Map.class, CoderProviders.fromStaticMethods(Map.class, MapCoder.class));
      builder.put(
          Metadata.class, CoderProviders.fromStaticMethods(Metadata.class, MetadataCoder.class));
      builder.put(
          ResourceId.class,
          CoderProviders.fromStaticMethods(ResourceId.class, ResourceIdCoder.class));
      builder.put(
          FileIO.ReadableFile.class,
          CoderProviders.fromStaticMethods(FileIO.ReadableFile.class, ReadableFileCoder.class));
      builder.put(Set.class, CoderProviders.fromStaticMethods(Set.class, SetCoder.class));
      builder.put(
          String.class, CoderProviders.fromStaticMethods(String.class, StringUtf8Coder.class));
      builder.put(
          TimestampedValue.class,
          CoderProviders.fromStaticMethods(
              TimestampedValue.class, TimestampedValue.TimestampedValueCoder.class));
      builder.put(Void.class, CoderProviders.fromStaticMethods(Void.class, VoidCoder.class));
      builder.put(
          byte[].class, CoderProviders.fromStaticMethods(byte[].class, ByteArrayCoder.class));
      builder.put(
          IntervalWindow.class,
          CoderProviders.forCoder(
              TypeDescriptor.of(IntervalWindow.class), IntervalWindow.getCoder()));
      commonTypesToCoderProviders = builder.build();
    }

    @Override
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      CoderProvider factory = commonTypesToCoderProviders.get(typeDescriptor.getRawType());
      if (factory == null) {
        throw new CannotProvideCoderException(
            String.format("%s is not one of the common types.", typeDescriptor));
      }
      return factory.coderFor(typeDescriptor, componentCoders);
    }
  }

  static {
    // Register the standard coders first so they are chosen over ServiceLoader ones
    List<CoderProvider> codersToRegister = new ArrayList<>();
    codersToRegister.add(new CommonTypes());

    // Enumerate all the CoderRegistrars in a deterministic order, adding all coders to register
    Set<CoderProviderRegistrar> registrars = Sets.newTreeSet(ObjectsClassComparator.INSTANCE);
    registrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(CoderProviderRegistrar.class, ReflectHelpers.findClassLoader())));

    // DefaultCoder should have the highest precedence and SerializableCoder the lowest
    codersToRegister.addAll(new DefaultCoder.DefaultCoderProviderRegistrar().getCoderProviders());
    for (CoderProviderRegistrar registrar : registrars) {
      codersToRegister.addAll(registrar.getCoderProviders());
    }
    codersToRegister.add(SerializableCoder.getCoderProvider());

    REGISTERED_CODER_FACTORIES = ImmutableList.copyOf(codersToRegister);
  }

  /**
   * Creates a CoderRegistry containing registrations for all standard coders part of the core Java
   * Apache Beam SDK and also any registrations provided by {@link CoderProviderRegistrar coder
   * registrars}.
   *
   * <p>Multiple registrations which can produce a coder for a given type result in a Coder created
   * by the (in order of precedence):
   *
   * <ul>
   *   <li>{@link CoderProvider coder providers} registered programmatically through {@link
   *       CoderRegistry#registerCoderProvider}.
   *   <li>{@link CoderProvider coder providers} for core types found within the Apache Beam Java
   *       SDK being used.
   *   <li>The {@link CoderProvider coder providers} from the {@link CoderProviderRegistrar} with
   *       the lexicographically smallest {@link Class#getName() class name} being used.
   * </ul>
   */
  public static CoderRegistry createDefault() {
    return new CoderRegistry();
  }

  private CoderRegistry() {
    coderProviders = new ArrayDeque<>(REGISTERED_CODER_FACTORIES);
  }

  /**
   * Registers {@code coderProvider} as a potential {@link CoderProvider} which can produce {@code
   * Coder} instances.
   *
   * <p>This method prioritizes this {@link CoderProvider} over all prior registered coders.
   *
   * <p>See {@link CoderProviders} for common {@link CoderProvider} patterns.
   */
  public void registerCoderProvider(CoderProvider coderProvider) {
    coderProviders.addFirst(coderProvider);
  }

  /**
   * Registers the provided {@link Coder} for the given class.
   *
   * <p>Note that this is equivalent to {@code registerCoderForType(TypeDescriptor.of(clazz))}. See
   * {@link #registerCoderForType(TypeDescriptor, Coder)} for further details.
   */
  public void registerCoderForClass(Class<?> clazz, Coder<?> coder) {
    registerCoderForType(TypeDescriptor.of(clazz), coder);
  }

  /**
   * Registers the provided {@link Coder} for the given type.
   *
   * <p>Note that this is equivalent to {@code registerCoderProvider(CoderProviders.forCoder(type,
   * coder))}. See {@link #registerCoderProvider} and {@link CoderProviders#forCoder} for further
   * details.
   */
  public void registerCoderForType(TypeDescriptor<?> type, Coder<?> coder) {
    registerCoderProvider(CoderProviders.forCoder(type, coder));
  }

  /**
   * Returns the {@link Coder} to use for values of the given class.
   *
   * @throws CannotProvideCoderException if a {@link Coder} cannot be provided
   */
  public <T> Coder<T> getCoder(Class<T> clazz) throws CannotProvideCoderException {
    return getCoder(TypeDescriptor.of(clazz));
  }

  /**
   * Returns the {@link Coder} to use for values of the given type.
   *
   * @throws CannotProvideCoderException if a {@link Coder} cannot be provided
   */
  public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
    return getCoderFromTypeDescriptor(type, HashMultimap.create());
  }

  /**
   * Returns the {@link Coder} for values of the given type, where the given input type uses the
   * given {@link Coder}.
   *
   * @throws CannotProvideCoderException if a {@link Coder} cannot be provided
   * @deprecated This method is to change in an unknown backwards incompatible way once support for
   *     this functionality is refined.
   */
  @Deprecated
  @Internal
  public <InputT, OutputT> Coder<OutputT> getCoder(
      TypeDescriptor<OutputT> typeDescriptor,
      TypeDescriptor<InputT> inputTypeDescriptor,
      Coder<InputT> inputCoder)
      throws CannotProvideCoderException {
    checkArgument(typeDescriptor != null);
    checkArgument(inputTypeDescriptor != null);
    checkArgument(inputCoder != null);
    return getCoderFromTypeDescriptor(
        typeDescriptor, getTypeToCoderBindings(inputTypeDescriptor.getType(), inputCoder));
  }

  /**
   * Returns the {@link Coder} to use on elements produced by this function, given the {@link Coder}
   * used for its input elements.
   *
   * @throws CannotProvideCoderException if a {@link Coder} cannot be provided
   * @deprecated This method is to change in an unknown backwards incompatible way once support for
   *     this functionality is refined.
   */
  @Deprecated
  @Internal
  public <InputT, OutputT> Coder<OutputT> getOutputCoder(
      SerializableFunction<InputT, OutputT> fn, Coder<InputT> inputCoder)
      throws CannotProvideCoderException {

    ParameterizedType fnType =
        (ParameterizedType)
            TypeDescriptor.of(fn.getClass()).getSupertype(SerializableFunction.class).getType();

    return getCoder(
        fn.getClass(),
        SerializableFunction.class,
        ImmutableMap.of(fnType.getActualTypeArguments()[0], inputCoder),
        SerializableFunction.class.getTypeParameters()[1]);
  }

  /**
   * Returns the {@link Coder} to use for the specified type parameter specialization of the
   * subclass, given {@link Coder Coders} to use for all other type parameters (if any).
   *
   * @throws CannotProvideCoderException if a {@link Coder} cannot be provided
   * @deprecated This method is to change in an unknown backwards incompatible way once support for
   *     this functionality is refined.
   */
  @Deprecated
  @Internal
  public <T, OutputT> Coder<OutputT> getCoder(
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
      throw new CannotProvideCoderException("Cannot infer coder for type parameter " + param);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a {@code Map} from each of {@code baseClass}'s type parameters to the {@link Coder} to
   * use for it, in the context of {@code subClass}'s specialization of {@code baseClass}.
   *
   * <p>If no {@link Coder} can be inferred for a particular type parameter, then that type variable
   * will be absent from the returned {@code Map}.
   *
   * <p>For example, if {@code baseClass} is {@code Map.class}, where {@code Map<K, V>} has type
   * parameters {@code K} and {@code V}, and {@code subClass} extends {@code Map<String, Integer>}
   * then the result will map the type variable {@code K} to a {@code Coder<String>} and the type
   * variable {@code V} to a {@code Coder<Integer>}.
   *
   * <p>The {@code knownCoders} parameter can be used to provide known {@link Coder Coders} for any
   * of the parameters; these will be used to infer the others.
   *
   * <p>Note that inference is attempted for every type variable. For a type {@code MyType<One, Two,
   * Three>} inference will be attempted for all of {@code One}, {@code Two}, {@code Three}, even if
   * the requester only wants a {@link Coder} for {@code Two}.
   *
   * <p>For this reason {@code getDefaultCoders} (plural) does not throw an exception if a {@link
   * Coder} for a particular type variable cannot be inferred, but merely omits the entry from the
   * returned {@code Map}. It is the responsibility of the caller (usually {@link
   * #getCoderFromTypeDescriptor} to extract the desired coder or throw a {@link
   * CannotProvideCoderException} when appropriate.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders a map corresponding to the set of known {@link Coder Coders} indexed by
   *     parameter name
   */
  private <T> Map<Type, Coder<?>> getDefaultCoders(
      Class<? extends T> subClass, Class<T> baseClass, Map<Type, ? extends Coder<?>> knownCoders) {
    TypeVariable<Class<T>>[] typeParams = baseClass.getTypeParameters();
    Coder<?>[] knownCodersArray = new Coder<?>[typeParams.length];
    for (int i = 0; i < typeParams.length; i++) {
      knownCodersArray[i] = knownCoders.get(typeParams[i]);
    }
    Coder<?>[] resultArray = getDefaultCoders(subClass, baseClass, knownCodersArray);
    Map<Type, Coder<?>> result = new HashMap<>();
    for (int i = 0; i < typeParams.length; i++) {
      if (resultArray[i] != null) {
        result.put(typeParams[i], resultArray[i]);
      }
    }
    return result;
  }

  /**
   * Returns an array listing, for each of {@code baseClass}'s type parameters, the {@link Coder} to
   * use for it, in the context of {@code subClass}'s specialization of {@code baseClass}.
   *
   * <p>If a {@link Coder} cannot be inferred for a type variable, its slot in the resulting array
   * will be {@code null}.
   *
   * <p>For example, if {@code baseClass} is {@code Map.class}, where {@code Map<K, V>} has type
   * parameters {@code K} and {@code V} in that order, and {@code subClass} extends {@code
   * Map<String, Integer>} then the result will contain a {@code Coder<String>} and a {@code
   * Coder<Integer>}, in that order.
   *
   * <p>The {@code knownCoders} parameter can be used to provide known {@link Coder Coders} for any
   * of the type parameters. These will be used to infer the others. If non-null, the length of this
   * array must match the number of type parameters of {@code baseClass}, and simply be filled with
   * {@code null} values for each type parameters without a known {@link Coder}.
   *
   * <p>Note that inference is attempted for every type variable. For a type {@code MyType<One, Two,
   * Three>} inference will will be attempted for all of {@code One}, {@code Two}, {@code Three},
   * even if the requester only wants a {@link Coder} for {@code Two}.
   *
   * <p>For this reason {@code getDefaultCoders} (plural) does not throw an exception if a {@link
   * Coder} for a particular type variable cannot be inferred. Instead, it results in a {@code null}
   * in the array. It is the responsibility of the caller (usually {@link
   * #getCoderFromTypeDescriptor} to extract the desired coder or throw a {@link
   * CannotProvideCoderException} when appropriate.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders an array corresponding to the set of base class type parameters. Each entry
   *     can be either a {@link Coder} (in which case it will be used for inference) or {@code null}
   *     (in which case it will be inferred). May be {@code null} to indicate the entire set of
   *     parameters should be inferred.
   * @throws IllegalArgumentException if baseClass doesn't have type parameters or if the length of
   *     {@code knownCoders} is not equal to the number of type parameters of {@code baseClass}.
   */
  private <T> Coder<?>[] getDefaultCoders(
      Class<? extends T> subClass, Class<T> baseClass, @Nullable Coder<?>[] knownCoders) {
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
          String.format(
              "Class %s has %d parameters, but %d coders are requested.",
              baseClass.getCanonicalName(), typeArgs.length, knownCoders.length));
    }

    SetMultimap<Type, Coder<?>> context = HashMultimap.create();
    for (int i = 0; i < knownCoders.length; i++) {
      if (knownCoders[i] != null) {
        try {
          verifyCompatible(knownCoders[i], typeArgs[i]);
        } catch (IncompatibleCoderException exn) {
          throw new IllegalArgumentException(
              String.format(
                  "Provided coders for type arguments of %s contain incompatibilities:"
                      + " Cannot encode elements of type %s with coder %s",
                  baseClass, typeArgs[i], knownCoders[i]),
              exn);
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
          result[i] = getCoderFromTypeDescriptor(TypeDescriptor.of(typeArgs[i]), context);
        } catch (CannotProvideCoderException exc) {
          result[i] = null;
        }
      }
    }
    return result;
  }

  /**
   * Thrown when a {@link Coder} cannot possibly encode a type, yet has been proposed as a {@link
   * Coder} for that type.
   */
  @VisibleForTesting
  static class IncompatibleCoderException extends RuntimeException {
    private Coder<?> coder;
    private Type type;

    IncompatibleCoderException(String message, Coder<?> coder, Type type) {
      super(message);
      this.coder = coder;
      this.type = type;
    }

    IncompatibleCoderException(String message, Coder<?> coder, Type type, Throwable cause) {
      super(message, cause);
      this.coder = coder;
      this.type = type;
    }

    public Coder<?> getCoder() {
      return coder;
    }

    public Type getType() {
      return type;
    }
  }

  /**
   * Returns {@code true} if the given {@link Coder} can possibly encode elements of the given type.
   */
  @VisibleForTesting
  static <T, CoderT extends Coder<T>, CandidateT> void verifyCompatible(
      CoderT coder, Type candidateType) throws IncompatibleCoderException {

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
        (TypeDescriptor<CandidateT>) TypeDescriptor.of(candidateType);
    @SuppressWarnings("unchecked")
    Class<CandidateT> candidateClass = (Class<CandidateT>) candidateDescriptor.getRawType();

    // If coder has type Coder<T> where the actual value of T is lost
    // to erasure, then we cannot rule it out.
    if (candidateType instanceof TypeVariable) {
      return;
    }

    // If the raw types are not compatible, we can certainly rule out
    // coder compatibility
    if (!codedClass.isAssignableFrom(candidateClass)) {
      throw new IncompatibleCoderException(
          String.format(
              "Cannot encode elements of type %s with coder %s because the"
                  + " coded type %s is not assignable from %s",
              candidateType, coder, codedClass, candidateType),
          coder,
          candidateType);
    }
    // we have established that this is a covariant upcast... though
    // coders are invariant, we are just checking one direction
    @SuppressWarnings("unchecked")
    TypeDescriptor<T> candidateOkDescriptor = (TypeDescriptor<T>) candidateDescriptor;

    // If the coded type is a parameterized type where any of the actual
    // type parameters are not compatible, then the whole thing is certainly not
    // compatible.
    if ((codedType instanceof ParameterizedType) && !isNullOrEmpty(coder.getCoderArguments())) {
      ParameterizedType parameterizedSupertype =
          (ParameterizedType) candidateOkDescriptor.getSupertype(codedClass).getType();
      Type[] typeArguments = parameterizedSupertype.getActualTypeArguments();
      List<? extends Coder<?>> typeArgumentCoders = coder.getCoderArguments();
      if (typeArguments.length < typeArgumentCoders.size()) {
        throw new IncompatibleCoderException(
            String.format(
                "Cannot encode elements of type %s with coder %s:"
                    + " the generic supertype %s has %s type parameters, which is less than the"
                    + " number of coder arguments %s has (%s).",
                candidateOkDescriptor,
                coder,
                parameterizedSupertype,
                typeArguments.length,
                coder,
                typeArgumentCoders.size()),
            coder,
            candidateOkDescriptor.getType());
      }
      for (int i = 0; i < typeArgumentCoders.size(); i++) {
        try {
          Coder<?> typeArgumentCoder = typeArgumentCoders.get(i);
          verifyCompatible(
              typeArgumentCoder, candidateDescriptor.resolveType(typeArguments[i]).getType());
        } catch (IncompatibleCoderException exn) {
          throw new IncompatibleCoderException(
              String.format(
                  "Cannot encode elements of type %s with coder %s"
                      + " because some component coder is incompatible",
                  candidateType, coder),
              coder,
              candidateType,
              exn);
        }
      }
    }
  }

  private static boolean isNullOrEmpty(Collection<?> c) {
    return c == null || c.isEmpty();
  }

  /** The list of {@link CoderProvider coder providers} to use to provide Coders. */
  private ArrayDeque<CoderProvider> coderProviders;

  /**
   * Returns a {@link Coder} to use for values of the given type, in a context where the given types
   * use the given coders.
   *
   * @throws CannotProvideCoderException if a coder cannot be provided
   */
  private <T> Coder<T> getCoderFromTypeDescriptor(
      TypeDescriptor<T> typeDescriptor, SetMultimap<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {
    Type type = typeDescriptor.getType();
    Coder<?> coder;
    if (typeDescriptor.equals(TypeDescriptors.rows())) {
      throw new CannotProvideCoderException(
          "Cannot provide a coder for a Beam Row. Please provide a schema instead using PCollection.setRowSchema.");
    }
    if (typeCoderBindings.containsKey(type)) {
      Set<Coder<?>> coders = typeCoderBindings.get(type);
      if (coders.size() == 1) {
        coder = Iterables.getOnlyElement(coders);
      } else {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide a coder for type variable %s"
                    + " because the actual type is over specified by multiple"
                    + " incompatible coders %s.",
                type, coders),
            ReasonCode.OVER_SPECIFIED);
      }
    } else if (type instanceof Class<?>) {
      coder = getCoderFromFactories(typeDescriptor, Collections.emptyList());
    } else if (type instanceof ParameterizedType) {
      coder = getCoderFromParameterizedType((ParameterizedType) type, typeCoderBindings);
    } else if (type instanceof TypeVariable) {
      coder = getCoderFromFactories(typeDescriptor, Collections.emptyList());
    } else if (type instanceof WildcardType) {
      // No coder for an unknown generic type.
      throw new CannotProvideCoderException(
          String.format("Cannot provide a coder for wildcard type %s.", type), ReasonCode.UNKNOWN);
    } else {
      throw new RuntimeException("Internal error: unexpected kind of Type: " + type);
    }

    LOG.debug("Coder for {}: {}", typeDescriptor, coder);
    @SuppressWarnings("unchecked")
    Coder<T> result = (Coder<T>) coder;
    return result;
  }

  /**
   * Returns a {@link Coder} to use for values of the given parameterized type, in a context where
   * the given types use the given {@link Coder Coders}.
   *
   * @throws CannotProvideCoderException if no coder can be provided
   */
  private Coder<?> getCoderFromParameterizedType(
      ParameterizedType type, SetMultimap<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {

    List<Coder<?>> typeArgumentCoders = new ArrayList<>();
    for (Type typeArgument : type.getActualTypeArguments()) {
      try {
        Coder<?> typeArgumentCoder =
            getCoderFromTypeDescriptor(TypeDescriptor.of(typeArgument), typeCoderBindings);
        typeArgumentCoders.add(typeArgumentCoder);
      } catch (CannotProvideCoderException exc) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide coder for parameterized type %s: %s", type, exc.getMessage()),
            exc);
      }
    }
    return getCoderFromFactories(TypeDescriptor.of(type), typeArgumentCoders);
  }

  /**
   * Attempts to create a {@link Coder} from any registered {@link CoderProvider} returning the
   * first successfully created instance.
   */
  private Coder<?> getCoderFromFactories(
      TypeDescriptor<?> typeDescriptor, List<Coder<?>> typeArgumentCoders)
      throws CannotProvideCoderException {
    List<CannotProvideCoderException> suppressedExceptions = new ArrayList<>();
    for (CoderProvider coderProvider : coderProviders) {
      try {
        return coderProvider.coderFor(typeDescriptor, typeArgumentCoders);
      } catch (CannotProvideCoderException e) {
        // Add all failures as suppressed exceptions.
        suppressedExceptions.add(e);
      }
    }

    // Build up the error message and list of causes.
    StringBuilder messageBuilder =
        new StringBuilder()
            .append("Unable to provide a Coder for ")
            .append(typeDescriptor)
            .append(".\n")
            .append("  Building a Coder using a registered CoderProvider failed.\n")
            .append("  See suppressed exceptions for detailed failures.");
    CannotProvideCoderException exceptionOnFailure =
        new CannotProvideCoderException(messageBuilder.toString());
    for (CannotProvideCoderException suppressedException : suppressedExceptions) {
      exceptionOnFailure.addSuppressed(suppressedException);
    }
    throw exceptionOnFailure;
  }

  /**
   * Returns an immutable {@code SetMultimap} from each of the type variables embedded in the given
   * type to the corresponding types in the given {@link Coder}.
   */
  private SetMultimap<Type, Coder<?>> getTypeToCoderBindings(Type type, Coder<?> coder) {
    checkArgument(type != null);
    checkArgument(coder != null);
    if (type instanceof TypeVariable || type instanceof Class) {
      return ImmutableSetMultimap.of(type, coder);
    } else if (type instanceof ParameterizedType) {
      return getTypeToCoderBindings((ParameterizedType) type, coder);
    } else {
      return ImmutableSetMultimap.of();
    }
  }

  /**
   * Returns an immutable {@code SetMultimap} from the type arguments of the parameterized type to
   * their corresponding {@link Coder Coders}, and so on recursively for their type parameters.
   *
   * <p>This method is simply a specialization to break out the most elaborate case of {@link
   * #getTypeToCoderBindings(Type, Coder)}.
   */
  private SetMultimap<Type, Coder<?>> getTypeToCoderBindings(
      ParameterizedType type, Coder<?> coder) {
    List<Type> typeArguments = Arrays.asList(type.getActualTypeArguments());
    List<? extends Coder<?>> coderArguments = coder.getCoderArguments();

    if ((coderArguments == null) || (typeArguments.size() != coderArguments.size())) {
      return ImmutableSetMultimap.of();
    } else {
      SetMultimap<Type, Coder<?>> typeToCoder = HashMultimap.create();

      typeToCoder.put(type, coder);

      for (int i = 0; i < typeArguments.size(); i++) {
        Type typeArgument = typeArguments.get(i);
        Coder<?> coderArgument = coderArguments.get(i);
        if (coderArgument != null) {
          typeToCoder.putAll(getTypeToCoderBindings(typeArgument, coderArgument));
        }
      }

      return ImmutableSetMultimap.<Type, Coder<?>>builder().putAll(typeToCoder).build();
    }
  }
}
