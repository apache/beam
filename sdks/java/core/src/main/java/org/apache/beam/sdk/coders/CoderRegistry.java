
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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.coders.CannotProvideCoderException.ReasonCode;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Set;

import javax.annotation.Nullable;

/**
 * A {@link CoderRegistry} allows registering the default {@link Coder} to use for a Java class,
 * and looking up and instantiating the default {@link Coder} for a Java type.
 *
 * <p>{@link CoderRegistry} uses the following mechanisms to determine a default {@link Coder} for a
 * Java class, in order of precedence:
 * <ol>
 *   <li>Registration:
 *     <ul>
 *       <li>A {@link CoderFactory} can be registered to handle a particular class via
 *           {@link #registerCoder(Class, CoderFactory)}.</li>
 *       <li>A {@link Coder} class with the static methods to satisfy
 *           {@link CoderFactories#fromStaticMethods} can be registered via
 *           {@link #registerCoder(Class, Class)}.</li>
 *       <li>Built-in types are registered via
 *           {@link #registerStandardCoders()}.</li>
 *     </ul>
 *   <li>Annotations: {@link DefaultCoder} can be used to annotate a type with
 *       the default {@code Coder} type. The {@link Coder} class must satisfy the requirements
 *       of {@link CoderProviders#fromStaticMethods}.
 *   <li>Fallback: A fallback {@link CoderProvider} is used to attempt to provide a {@link Coder}
 *       for any type. By default, there are two chained fallback coders:
 *       {@link ProtoCoder#coderProvider}, which can provide a coder to efficiently serialize any
 *       Protocol Buffers message, and then {@link SerializableCoder#PROVIDER}, which can provide a
 *       {@link Coder} for any type that is serializable via Java serialization. The fallback
 *       {@link CoderProvider} can be get and set respectively using
 *       {@link #getFallbackCoderProvider()} and {@link #setFallbackCoderProvider}. Multiple
 *       fallbacks can be chained together using {@link CoderProviders#firstOf}.
 * </ol>
 */
public class CoderRegistry implements CoderProvider {

  private static final Logger LOG = LoggerFactory.getLogger(CoderRegistry.class);

  public CoderRegistry() {
    setFallbackCoderProvider(
        CoderProviders.firstOf(ProtoCoder.coderProvider(), SerializableCoder.PROVIDER));
  }

  /**
   * Registers standard Coders with this CoderRegistry.
   */
  public void registerStandardCoders() {
    registerCoder(Byte.class, ByteCoder.class);
    registerCoder(ByteString.class, ByteStringCoder.class);
    registerCoder(Double.class, DoubleCoder.class);
    registerCoder(Instant.class, InstantCoder.class);
    registerCoder(Integer.class, VarIntCoder.class);
    registerCoder(Iterable.class, IterableCoder.class);
    registerCoder(KV.class, KvCoder.class);
    registerCoder(List.class, ListCoder.class);
    registerCoder(Long.class, VarLongCoder.class);
    registerCoder(Map.class, MapCoder.class);
    registerCoder(Set.class, SetCoder.class);
    registerCoder(String.class, StringUtf8Coder.class);
    registerCoder(TableRow.class, TableRowJsonCoder.class);
    registerCoder(TimestampedValue.class, TimestampedValue.TimestampedValueCoder.class);
    registerCoder(Void.class, VoidCoder.class);
    registerCoder(byte[].class, ByteArrayCoder.class);
  }

  /**
   * Registers {@code coderClazz} as the default {@link Coder} class to handle encoding and
   * decoding instances of {@code clazz}, overriding prior registrations if any exist.
   *
   * <p>Supposing {@code T} is the static type corresponding to the {@code clazz}, then
   * {@code coderClazz} should have a static factory method with the following signature:
   *
   * <pre> {@code
   * public static Coder<T> of(Coder<X> argCoder1, Coder<Y> argCoder2, ...)
   * } </pre>
   *
   * <p>This method will be called to create instances of {@code Coder<T>} for values of type
   * {@code T}, passing Coders for each of the generic type parameters of {@code T}.  If {@code T}
   * takes no generic type parameters, then the {@code of()} factory method should have no
   * arguments.
   *
   * <p>If {@code T} is a parameterized type, then it should additionally have a method with the
   * following signature:
   *
   * <pre> {@code
   * public static List<Object> getInstanceComponents(T exampleValue);
   * } </pre>
   *
   * <p>This method will be called to decompose a value during the {@link Coder} inference process,
   * to automatically choose {@link Coder Coders} for the components.
   *
   * @param clazz the class of objects to be encoded
   * @param coderClazz a class with static factory methods to provide {@link Coder Coders}
   */
  public void registerCoder(Class<?> clazz, Class<?> coderClazz) {
    registerCoder(clazz, CoderFactories.fromStaticMethods(coderClazz));
  }

  /**
   * Registers {@code coderFactory} as the default {@link CoderFactory} to produce {@code Coder}
   * instances to decode and encode instances of {@code clazz}. This will override prior
   * registrations if any exist.
   */
  public void registerCoder(Class<?> clazz, CoderFactory coderFactory) {
    coderFactoryMap.put(clazz, coderFactory);
  }

  /**
   * Register the provided {@link Coder} for encoding all values of the specified {@code Class}.
   * This will override prior registrations if any exist.
   *
   * <p>Not for use with generic rawtypes. Instead, register a {@link CoderFactory} via
   * {@link #registerCoder(Class, CoderFactory)} or ensure your {@code Coder} class has the
   * appropriate static methods and register it directly via {@link #registerCoder(Class, Class)}.
   */
  public <T> void registerCoder(Class<T> rawClazz, Coder<T> coder) {
    checkArgument(
        rawClazz.getTypeParameters().length == 0,
        "CoderRegistry.registerCoder(Class<T>, Coder<T>) may not be used "
            + "with unspecialized generic classes");

    CoderFactory factory = CoderFactories.forCoder(coder);
    registerCoder(rawClazz, factory);
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given type.
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
   * Returns the {@link Coder} to use by default for values of the given type, where the given input
   * type uses the given {@link Coder}.
   *
   * @throws CannotProvideCoderException if there is no default Coder.
   */
  public <InputT, OutputT> Coder<OutputT> getDefaultCoder(
      TypeDescriptor<OutputT> typeDescriptor,
      TypeDescriptor<InputT> inputTypeDescriptor,
      Coder<InputT> inputCoder)
      throws CannotProvideCoderException {
    return getDefaultCoder(
        typeDescriptor, getTypeToCoderBindings(inputTypeDescriptor.getType(), inputCoder));
  }

  /**
   * Returns the {@link Coder} to use on elements produced by this function, given the {@link Coder}
   * used for its input elements.
   */
  public <InputT, OutputT> Coder<OutputT> getDefaultOutputCoder(
      SerializableFunction<InputT, OutputT> fn, Coder<InputT> inputCoder)
      throws CannotProvideCoderException {

    ParameterizedType fnType = (ParameterizedType)
        TypeDescriptor.of(fn.getClass()).getSupertype(SerializableFunction.class).getType();

    return getDefaultCoder(
        fn.getClass(),
        SerializableFunction.class,
        ImmutableMap.of(fnType.getActualTypeArguments()[0], inputCoder),
        SerializableFunction.class.getTypeParameters()[1]);
  }

  /**
   * Returns the {@link Coder} to use for the specified type parameter specialization of the
   * subclass, given {@link Coder Coders} to use for all other type parameters (if any).
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
   * Returns the {@link Coder} to use for the provided example value, if it can be determined.
   *
   * @throws CannotProvideCoderException if there is no default {@link Coder} or
   * more than one {@link Coder} matches
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
        throw new CannotProvideCoderException(String.format(
            "Cannot provide coder based on value with class %s: The registered CoderFactory with "
            + "class %s failed to decompose the value, which is required in order to provide "
            + "Coders for the components.",
            clazz.getCanonicalName(), factory.getClass().getCanonicalName()));
      }

      // componentcoders = components.map(this.getDefaultCoder)
      List<Coder<?>> componentCoders = new ArrayList<>();
      for (Object component : components) {
        try {
          Coder<?> componentCoder = getDefaultCoder(component);
          componentCoders.add(componentCoder);
        } catch (CannotProvideCoderException exc) {
          throw new CannotProvideCoderException(
              String.format("Cannot provide coder based on value with class %s",
                  clazz.getCanonicalName()),
              exc);
        }
      }

      // Trust that factory.create maps from valid component Coders
      // to a valid Coder<T>.
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) factory.create(componentCoders);
      return coder;
    }
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given class. The following three
   * sources for a {@link Coder} will be attempted, in order:
   *
   * <ol>
   *   <li>A {@link Coder} class registered explicitly via a call to {@link #registerCoder},
   *   <li>A {@link DefaultCoder} annotation on the class,
   *   <li>This registry's fallback {@link CoderProvider}, which may be able to generate a
   *   {@link Coder} for an arbitrary class.
   * </ol>
   *
   * @throws CannotProvideCoderException if a {@link Coder} cannot be provided
   */
  public <T> Coder<T> getDefaultCoder(Class<T> clazz) throws CannotProvideCoderException {

    CannotProvideCoderException factoryException;
    try {
      CoderFactory coderFactory = getDefaultCoderFactory(clazz);
      LOG.debug("Default coder for {} found by factory", clazz);
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) coderFactory.create(Collections.<Coder<?>>emptyList());
      return coder;
    } catch (CannotProvideCoderException exc) {
      factoryException = exc;
    }

    CannotProvideCoderException annotationException;
    try {
      return getDefaultCoderFromAnnotation(clazz);
    } catch (CannotProvideCoderException exc) {
      annotationException = exc;
    }

    CannotProvideCoderException fallbackException;
    if (getFallbackCoderProvider() != null) {
      try {
        return getFallbackCoderProvider().getCoder(TypeDescriptor.<T>of(clazz));
      } catch (CannotProvideCoderException exc) {
        fallbackException = exc;
      }
    } else {
      fallbackException = new CannotProvideCoderException("no fallback CoderProvider configured");
    }

    // Build up the error message and list of causes.
    StringBuilder messageBuilder = new StringBuilder()
        .append("Unable to provide a default Coder for ").append(clazz.getCanonicalName())
        .append(". Correct one of the following root causes:");

    messageBuilder
        .append("\n  Building a Coder using a registered CoderFactory failed: ")
        .append(factoryException.getMessage());

    messageBuilder
        .append("\n  Building a Coder from the @DefaultCoder annotation failed: ")
        .append(annotationException.getMessage());

    messageBuilder
        .append("\n  Building a Coder from the fallback CoderProvider failed: ")
        .append(fallbackException.getMessage());

    throw new CannotProvideCoderException(messageBuilder.toString());
  }

  /**
   * Sets the fallback {@link CoderProvider} for this registry. If no other method succeeds in
   * providing a {@code Coder<T>} for a type {@code T}, then the registry will attempt to create
   * a {@link Coder} using this {@link CoderProvider}.
   *
   * <p>By default, this is set to the chain of {@link ProtoCoder#coderProvider()} and
   * {@link SerializableCoder#PROVIDER}.
   *
   * <p>See {@link #getFallbackCoderProvider}.
   */
  public void setFallbackCoderProvider(CoderProvider coderProvider) {
    fallbackCoderProvider = coderProvider;
  }

  /**
   * Returns the fallback {@link CoderProvider} for this registry.
   *
   * <p>See {@link #setFallbackCoderProvider}.
   */
  public CoderProvider getFallbackCoderProvider() {
    return fallbackCoderProvider;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a {@code Map} from each of {@code baseClass}'s type parameters to the {@link Coder} to
   * use by default for it, in the context of {@code subClass}'s specialization of
   * {@code baseClass}.
   *
   * <p>If no {@link Coder} can be inferred for a particular type parameter, then that type variable
   * will be absent from the returned {@code Map}.
   *
   * <p>For example, if {@code baseClass} is {@code Map.class}, where {@code Map<K, V>} has type
   * parameters {@code K} and {@code V}, and {@code subClass} extends {@code Map<String, Integer>}
   * then the result will map the type variable {@code K} to a {@code Coder<String>} and the
   * type variable {@code V} to a {@code Coder<Integer>}.
   *
   * <p>The {@code knownCoders} parameter can be used to provide known {@link Coder Coders} for any
   * of the parameters; these will be used to infer the others.
   *
   * <p>Note that inference is attempted for every type variable. For a type
   * {@code MyType<One, Two, Three>} inference will be attempted for all of {@code One},
   * {@code Two}, {@code Three}, even if the requester only wants a {@link Coder} for {@code Two}.
   *
   * <p>For this reason {@code getDefaultCoders} (plural) does not throw an exception if a
   * {@link Coder} for a particular type variable cannot be inferred, but merely omits the entry
   * from the returned {@code Map}. It is the responsibility of the caller (usually
   * {@link #getDefaultCoder} to extract the desired coder or throw a
   * {@link CannotProvideCoderException} when appropriate.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders a map corresponding to the set of known {@link Coder Coders} indexed by
   * parameter name
   */
  private <T> Map<Type, Coder<?>> getDefaultCoders(
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
   * Returns an array listing, for each of {@code baseClass}'s type parameters, the {@link Coder} to
   * use by default for it, in the context of {@code subClass}'s specialization of
   * {@code baseClass}.
   *
   * <p>If a {@link Coder} cannot be inferred for a type variable, its slot in the resulting array
   * will be {@code null}.
   *
   * <p>For example, if {@code baseClass} is {@code Map.class}, where {@code Map<K, V>} has type
   * parameters {@code K} and {@code V} in that order, and {@code subClass} extends
   * {@code Map<String, Integer>} then the result will contain a {@code Coder<String>} and a
   * {@code Coder<Integer>}, in that order.
   *
   * <p>The {@code knownCoders} parameter can be used to provide known {@link Coder Coders} for any
   * of the type parameters. These will be used to infer the others. If non-null, the length of this
   * array must match the number of type parameters of {@code baseClass}, and simply be filled with
   * {@code null} values for each type parameters without a known {@link Coder}.
   *
   * <p>Note that inference is attempted for every type variable. For a type
   * {@code MyType<One, Two, Three>} inference will will be attempted for all of {@code One},
   * {@code Two}, {@code Three}, even if the requester only wants a {@link Coder} for {@code Two}.
   *
   * <p>For this reason {@code getDefaultCoders} (plural) does not throw an exception if a
   * {@link Coder} for a particular type variable cannot be inferred. Instead, it results in a
   * {@code null} in the array. It is the responsibility of the caller (usually
   * {@link #getDefaultCoder} to extract the desired coder or throw a
   * {@link CannotProvideCoderException} when appropriate.
   *
   * @param subClass the concrete type whose specializations are being inferred
   * @param baseClass the base type, a parameterized class
   * @param knownCoders an array corresponding to the set of base class type parameters. Each entry
   *        can be either a {@link Coder} (in which case it will be used for inference) or
   *        {@code null} (in which case it will be inferred). May be {@code null} to indicate the
   *        entire set of parameters should be inferred.
   * @throws IllegalArgumentException if baseClass doesn't have type parameters or if the length of
   *         {@code knownCoders} is not equal to the number of type parameters of {@code baseClass}.
   */
  private <T> Coder<?>[] getDefaultCoders(
      Class<? extends T> subClass,
      Class<T> baseClass,
      @Nullable Coder<?>[] knownCoders) {
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
          String.format("Class %s has %d parameters, but %d coders are requested.",
              baseClass.getCanonicalName(), typeArgs.length, knownCoders.length));
    }

    Map<Type, Coder<?>> context = new HashMap<>();
    for (int i = 0; i < knownCoders.length; i++) {
      if (knownCoders[i] != null) {
        try {
          verifyCompatible(knownCoders[i], typeArgs[i]);
        } catch (IncompatibleCoderException exn) {
          throw new IllegalArgumentException(
              String.format("Provided coders for type arguments of %s contain incompatibilities:"
                  + " Cannot encode elements of type %s with coder %s",
                  baseClass,
                  typeArgs[i], knownCoders[i]),
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
          result[i] = getDefaultCoder(typeArgs[i], context);
        } catch (CannotProvideCoderException exc) {
          result[i] = null;
        }
      }
    }
    return result;
  }


  /**
   * Thrown when a {@link Coder} cannot possibly encode a type, yet has been proposed as a
   * {@link Coder} for that type.
   */
  @VisibleForTesting static class IncompatibleCoderException extends RuntimeException {
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
   * Returns {@code true} if the given {@link Coder} can possibly encode elements
   * of the given type.
   */
  @VisibleForTesting static <T, CoderT extends Coder<T>, CandidateT>
  void verifyCompatible(CoderT coder, Type candidateType) throws IncompatibleCoderException {

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
          String.format("Cannot encode elements of type %s with coder %s because the"
              + " coded type %s is not assignable from %s",
              candidateType, coder, codedClass, candidateType),
          coder, candidateType);
    }
    // we have established that this is a covariant upcast... though
    // coders are invariant, we are just checking one direction
    @SuppressWarnings("unchecked")
    TypeDescriptor<T> candidateOkDescriptor = (TypeDescriptor<T>) candidateDescriptor;

    // If the coded type is a parameterized type where any of the actual
    // type parameters are not compatible, then the whole thing is certainly not
    // compatible.
    if ((codedType instanceof ParameterizedType) && !isNullOrEmpty(coder.getCoderArguments())) {
      ParameterizedType parameterizedSupertype = ((ParameterizedType)
           candidateOkDescriptor.getSupertype(codedClass).getType());
      Type[] typeArguments = parameterizedSupertype.getActualTypeArguments();
      List<? extends Coder<?>> typeArgumentCoders = coder.getCoderArguments();
      if (typeArguments.length < typeArgumentCoders.size()) {
        throw new IncompatibleCoderException(
            String.format("Cannot encode elements of type %s with coder %s:"
                + " the generic supertype %s has %s type parameters, which is less than the"
                + " number of coder arguments %s has (%s).",
                candidateOkDescriptor, coder,
                parameterizedSupertype, typeArguments.length,
                coder, typeArgumentCoders.size()),
            coder, candidateOkDescriptor.getType());
      }
      for (int i = 0; i < typeArgumentCoders.size(); i++) {
        try {
          verifyCompatible(
              typeArgumentCoders.get(i),
              candidateDescriptor.resolveType(typeArguments[i]).getType());
        } catch (IncompatibleCoderException exn) {
          throw new IncompatibleCoderException(
              String.format("Cannot encode elements of type %s with coder %s"
                  + " because some component coder is incompatible",
                  candidateType, coder),
              coder, candidateType, exn);
        }
      }
    }
  }

  private static boolean isNullOrEmpty(Collection<?> c) {
    return c == null || c.size() == 0;
  }

  /**
   * The map of classes to the CoderFactories to use to create their
   * default Coders.
   */
  private Map<Class<?>, CoderFactory> coderFactoryMap = new HashMap<>();

  /**
   * A provider of coders for types where no coder is registered.
   */
  private CoderProvider fallbackCoderProvider;

  /**
   * Returns the {@link CoderFactory} to use to create default {@link Coder Coders} for instances of
   * the given class, or {@code null} if there is no default {@link CoderFactory} registered.
   */
  private CoderFactory getDefaultCoderFactory(Class<?> clazz) throws CannotProvideCoderException {
    CoderFactory coderFactoryOrNull = coderFactoryMap.get(clazz);
    if (coderFactoryOrNull != null) {
      return coderFactoryOrNull;
    } else {
      throw new CannotProvideCoderException(
          String.format("Cannot provide coder based on value with class %s: No CoderFactory has "
              + "been registered for the class.", clazz.getCanonicalName()));
    }
  }

  /**
   * Returns the {@link Coder} returned according to the {@link CoderProvider} from any
   * {@link DefaultCoder} annotation on the given class.
   */
  private <T> Coder<T> getDefaultCoderFromAnnotation(Class<T> clazz)
      throws CannotProvideCoderException {
    DefaultCoder defaultAnnotation = clazz.getAnnotation(DefaultCoder.class);
    if (defaultAnnotation == null) {
      throw new CannotProvideCoderException(
          String.format("Class %s does not have a @DefaultCoder annotation.",
              clazz.getCanonicalName()));
    }

    LOG.debug("DefaultCoder annotation found for {} with value {}",
        clazz, defaultAnnotation.value());
    CoderProvider coderProvider = CoderProviders.fromStaticMethods(defaultAnnotation.value());
    return coderProvider.getCoder(TypeDescriptor.of(clazz));
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given type,
   * in a context where the given types use the given coders.
   *
   * @throws CannotProvideCoderException if a coder cannot be provided
   */
  private <T> Coder<T> getDefaultCoder(
      TypeDescriptor<T> typeDescriptor,
      Map<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {

    Coder<?> defaultCoder = getDefaultCoder(typeDescriptor.getType(), typeCoderBindings);
    LOG.debug("Default coder for {}: {}", typeDescriptor, defaultCoder);
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
  private Coder<?> getDefaultCoder(Type type, Map<Type, Coder<?>> typeCoderBindings)
      throws CannotProvideCoderException {
    Coder<?> coder = typeCoderBindings.get(type);
    if (coder != null) {
      return coder;
    }
    if (type instanceof Class<?>) {
      Class<?> clazz = (Class<?>) type;
      return getDefaultCoder(clazz);
    } else if (type instanceof ParameterizedType) {
      return getDefaultCoder((ParameterizedType) type, typeCoderBindings);
    } else if (type instanceof TypeVariable) {
      return getDefaultCoder(TypeDescriptor.of(type).getRawType());
    } else if (type instanceof WildcardType) {
      // No default coder for an unknown generic type.
      throw new CannotProvideCoderException(
          String.format("Cannot provide a coder for type variable %s"
          + " (declared by %s) because the actual type is unknown due to erasure.",
          type,
          ((TypeVariable<?>) type).getGenericDeclaration()),
          ReasonCode.TYPE_ERASURE);
    } else {
      throw new RuntimeException(
          "Internal error: unexpected kind of Type: " + type);
    }
  }

  /**
   * Returns the {@link Coder} to use by default for values of the given
   * parameterized type, in a context where the given types use the
   * given {@link Coder Coders}.
   *
   * @throws CannotProvideCoderException if no coder can be provided
   */
  private Coder<?> getDefaultCoder(
      ParameterizedType type,
      Map<Type, Coder<?>> typeCoderBindings)
          throws CannotProvideCoderException {

    CannotProvideCoderException factoryException;
    try {
      return getDefaultCoderFromFactory(type, typeCoderBindings);
    } catch (CannotProvideCoderException exc) {
      factoryException = exc;
    }

    CannotProvideCoderException annotationException;
    try {
      Class<?> rawClazz = (Class<?>) type.getRawType();
      return getDefaultCoderFromAnnotation(rawClazz);
    } catch (CannotProvideCoderException exc) {
      annotationException = exc;
    }

    // Build up the error message and list of causes.
    StringBuilder messageBuilder = new StringBuilder()
        .append("Unable to provide a default Coder for ").append(type)
        .append(". Correct one of the following root causes:");

    messageBuilder
        .append("\n  Building a Coder using a registered CoderFactory failed: ")
        .append(factoryException.getMessage());

    messageBuilder
        .append("\n  Building a Coder from the @DefaultCoder annotation failed: ")
        .append(annotationException.getMessage());

    throw new CannotProvideCoderException(messageBuilder.toString());
  }

  private Coder<?> getDefaultCoderFromFactory(
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
          String.format("Cannot provide coder for parameterized type %s: %s",
              type,
              exc.getMessage()),
          exc);
      }
    }
    return coderFactory.create(typeArgumentCoders);
  }

  /**
   * Returns an immutable {@code Map} from each of the type variables
   * embedded in the given type to the corresponding types
   * in the given {@link Coder}.
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
   * Returns an immutable {@code Map} from the type arguments of the parameterized type to their
   * corresponding {@link Coder Coders}, and so on recursively for their type parameters.
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
