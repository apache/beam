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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A utility class for creating {@link TypeDescriptor} objects for different types, such as Java
 * primitive types, containers and {@link KV KVs} of other {@link TypeDescriptor} objects, and
 * extracting type variables of parameterized types (e.g. extracting the {@code OutputT} type
 * variable of a {@code DoFn<InputT, OutputT>}).
 */
public class TypeDescriptors {
  /**
   * The {@link TypeDescriptor} for Boolean. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Boolean&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Boolean
   */
  public static TypeDescriptor<Boolean> booleans() {
    return new TypeDescriptor<Boolean>() {};
  }

  /**
   * The {@link TypeDescriptor} for Double. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Double&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Double
   */
  public static TypeDescriptor<Double> doubles() {
    return new TypeDescriptor<Double>() {};
  }

  /**
   * The {@link TypeDescriptor} for Float. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Float&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Float
   */
  public static TypeDescriptor<Float> floats() {
    return new TypeDescriptor<Float>() {};
  }

  /**
   * The {@link TypeDescriptor} for Integer. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Integer&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Integer
   */
  public static TypeDescriptor<Integer> integers() {
    return new TypeDescriptor<Integer>() {};
  }

  /**
   * The {@link TypeDescriptor} for Long. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Long&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Long
   */
  public static TypeDescriptor<Long> longs() {
    return new TypeDescriptor<Long>() {};
  }

  /**
   * The {@link TypeDescriptor} for Short. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Short&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Short
   */
  public static TypeDescriptor<Short> shorts() {
    return new TypeDescriptor<Short>() {};
  }

  /**
   * The {@link TypeDescriptor} for BigDecimal. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;BigDecimal&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for BigDecimal
   */
  public static TypeDescriptor<BigDecimal> bigdecimals() {
    return new TypeDescriptor<BigDecimal>() {};
  }

  /**
   * The {@link TypeDescriptor} for BigInteger. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;BigInteger&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for BigInteger
   */
  public static TypeDescriptor<BigInteger> bigintegers() {
    return new TypeDescriptor<BigInteger>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link Row}. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Row&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Row
   */
  public static TypeDescriptor<Row> rows() {
    return new TypeDescriptor<Row>() {};
  }

  /**
   * The {@link TypeDescriptor} for String. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;String&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for String
   */
  public static TypeDescriptor<String> strings() {
    return new TypeDescriptor<String>() {};
  }

  /**
   * The {@link TypeDescriptor} for Character. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Character&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Character
   */
  public static TypeDescriptor<Character> characters() {
    return new TypeDescriptor<Character>() {};
  }

  /**
   * The {@link TypeDescriptor} for Byte. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Byte&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for Byte
   */
  public static TypeDescriptor<Byte> bytes() {
    return new TypeDescriptor<Byte>() {};
  }

  /**
   * The {@link TypeDescriptor} for nulls/Void. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Void&gt;() {};
   * </pre>
   *
   * @return A {@link TypeDescriptor} for nulls/Void
   */
  public static TypeDescriptor<Void> nulls() {
    return new TypeDescriptor<Void>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV}. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;KV&lt;K,V&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.kv(TypeDescriptors.strings(), TypeDescriptors.strings()))
   *         .via(...));
   * }</pre>
   *
   * @param key The {@link TypeDescriptor} for the key
   * @param value The {@link TypeDescriptor} for the value
   * @return A {@link TypeDescriptor} for {@link KV}
   */
  public static <K, V> TypeDescriptor<KV<K, V>> kvs(
      TypeDescriptor<K> key, TypeDescriptor<V> value) {
    return new TypeDescriptor<KV<K, V>>() {}.where(new TypeParameter<K>() {}, key)
        .where(new TypeParameter<V>() {}, value);
  }

  /**
   * The {@link TypeDescriptor} for {@link Set}. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Set&lt;E&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<Set<String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.sets(TypeDescriptors.strings()))
   *         .via(...));
   * }</pre>
   *
   * @param element The {@link TypeDescriptor} for the set
   * @return A {@link TypeDescriptor} for {@link Set}
   */
  public static <T> TypeDescriptor<Set<T>> sets(TypeDescriptor<T> element) {
    return new TypeDescriptor<Set<T>>() {}.where(new TypeParameter<T>() {}, element);
  }

  /** The {@link TypeDescriptor} for {@link Map}. */
  public static <K, V> TypeDescriptor<Map<K, V>> maps(
      TypeDescriptor<K> keyType, TypeDescriptor<V> valueType) {
    return new TypeDescriptor<Map<K, V>>() {}.where(new TypeParameter<K>() {}, keyType)
        .where(new TypeParameter<V>() {}, valueType);
  }

  /**
   * The {@link TypeDescriptor} for {@link List}. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;List&lt;E&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<List<String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.lists(TypeDescriptors.strings()))
   *         .via(...));
   * }</pre>
   *
   * @param element The {@link TypeDescriptor} for the list
   * @return A {@link TypeDescriptor} for {@link List}
   */
  public static <T> TypeDescriptor<List<T>> lists(TypeDescriptor<T> element) {
    return new TypeDescriptor<List<T>>() {}.where(new TypeParameter<T>() {}, element);
  }

  /**
   * The {@link TypeDescriptor} for {@link Iterable}. This is the equivalent of:
   *
   * <pre>
   * new TypeDescriptor&lt;Iterable&lt;E&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<Iterable<String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.iterables(TypeDescriptors.strings()))
   *         .via(...));
   * }</pre>
   *
   * @param iterable The {@link TypeDescriptor} for the iterable
   * @return A {@link TypeDescriptor} for {@link Iterable}
   */
  public static <T> TypeDescriptor<Iterable<T>> iterables(TypeDescriptor<T> iterable) {
    return new TypeDescriptor<Iterable<T>>() {}.where(new TypeParameter<T>() {}, iterable);
  }

  public static TypeDescriptor<Void> voids() {
    return new TypeDescriptor<Void>() {};
  }

  /**
   * A helper interface for use with {@link #extractFromTypeParameters(Object, Class,
   * TypeVariableExtractor)}.
   */
  public interface TypeVariableExtractor<InputT, OutputT> {}

  /**
   * Extracts a type from the actual type parameters of a parameterized class, subject to Java type
   * erasure. The type to extract is specified in a way that is safe w.r.t. changing the type
   * signature of the parameterized class, as opposed to specifying the name or index of a type
   * variable.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * class Foo<BarT> {
   *   private ProcessFunction<BarT, String> fn;
   *
   *   TypeDescriptor<BarT> inferBarTypeDescriptorFromFn() {
   *     return TypeDescriptors.extractFromTypeParameters(
   *       fn,
   *       ProcessFunction.class,
   *       // The actual type of "fn" is matched against the input type of the extractor,
   *       // and the obtained values of type variables of the superclass are substituted
   *       // into the output type of the extractor.
   *       new TypeVariableExtractor<ProcessFunction<BarT, String>, BarT>() {});
   *   }
   * }
   * }</pre>
   *
   * @param instance The object being analyzed
   * @param supertype Parameterized superclass of interest
   * @param extractor A class for specifying the type to extract from the supertype
   * @return A {@link TypeDescriptor} for the actual value of the result type of the extractor,
   *     potentially containing unresolved type variables if the type was erased.
   */
  @SuppressWarnings("unchecked")
  public static <T, V> TypeDescriptor<V> extractFromTypeParameters(
      T instance, Class<? super T> supertype, TypeVariableExtractor<T, V> extractor) {
    return extractFromTypeParameters(
        (TypeDescriptor<T>) TypeDescriptor.of(instance.getClass()), supertype, extractor);
  }

  /**
   * Like {@link #extractFromTypeParameters(Object, Class, TypeVariableExtractor)}, but takes a
   * {@link TypeDescriptor} of the instance being analyzed rather than the instance itself.
   */
  @SuppressWarnings("unchecked")
  public static <T, V> TypeDescriptor<V> extractFromTypeParameters(
      TypeDescriptor<T> type, Class<? super T> supertype, TypeVariableExtractor<T, V> extractor) {
    // Get the type signature of the extractor, e.g.
    // TypeVariableExtractor<ProcessFunction<BarT, String>, BarT>
    TypeDescriptor<TypeVariableExtractor<T, V>> extractorSupertype =
        (TypeDescriptor<TypeVariableExtractor<T, V>>)
            TypeDescriptor.of(extractor.getClass()).getSupertype(TypeVariableExtractor.class);

    // Get the actual type argument, e.g. ProcessFunction<BarT, String>
    Type inputT = ((ParameterizedType) extractorSupertype.getType()).getActualTypeArguments()[0];

    // Get the actual supertype of the type being analyzed, hopefully with all type parameters
    // resolved, e.g. ProcessFunction<Integer, String>
    TypeDescriptor supertypeDescriptor = type.getSupertype(supertype);

    // Substitute actual supertype into the extractor, e.g.
    // TypeVariableExtractor<ProcessFunction<Integer, String>, Integer>
    TypeDescriptor<TypeVariableExtractor<T, V>> extractorT =
        extractorSupertype.where(inputT, supertypeDescriptor.getType());

    // Get output of the extractor.
    Type outputT = ((ParameterizedType) extractorT.getType()).getActualTypeArguments()[1];
    return (TypeDescriptor<V>) TypeDescriptor.of(outputT);
  }

  /**
   * Returns a type descriptor for the input of the given {@link ProcessFunction}, subject to Java
   * type erasure: may contain unresolved type variables if the type was erased.
   */
  public static <InputT, OutputT> TypeDescriptor<InputT> inputOf(
      ProcessFunction<InputT, OutputT> fn) {
    return extractFromTypeParameters(
        fn,
        ProcessFunction.class,
        new TypeVariableExtractor<ProcessFunction<InputT, OutputT>, InputT>() {});
  }

  /** Binary compatibility adapter for {@link #inputOf(ProcessFunction)}. */
  public static <InputT, OutputT> TypeDescriptor<InputT> inputOf(
      SerializableFunction<InputT, OutputT> fn) {
    return inputOf((ProcessFunction<InputT, OutputT>) fn);
  }

  /**
   * Returns a type descriptor for the output of the given {@link ProcessFunction}, subject to Java
   * type erasure: may contain unresolved type variables if the type was erased.
   */
  public static <InputT, OutputT> TypeDescriptor<OutputT> outputOf(
      ProcessFunction<InputT, OutputT> fn) {
    return extractFromTypeParameters(
        fn,
        ProcessFunction.class,
        new TypeVariableExtractor<ProcessFunction<InputT, OutputT>, OutputT>() {});
  }

  /** Binary compatibility adapter for {@link #outputOf(ProcessFunction)}. */
  public static <InputT, OutputT> TypeDescriptor<OutputT> outputOf(
      SerializableFunction<InputT, OutputT> fn) {
    return outputOf((ProcessFunction<InputT, OutputT>) fn);
  }

  /** Like {@link #inputOf(ProcessFunction)} but for {@link Contextful.Fn}. */
  public static <InputT, OutputT> TypeDescriptor<InputT> inputOf(
      Contextful.Fn<InputT, OutputT> fn) {
    return TypeDescriptors.extractFromTypeParameters(
        fn,
        Contextful.Fn.class,
        new TypeDescriptors.TypeVariableExtractor<Contextful.Fn<InputT, OutputT>, InputT>() {});
  }

  /** Like {@link #outputOf(ProcessFunction)} but for {@link Contextful.Fn}. */
  public static <InputT, OutputT> TypeDescriptor<OutputT> outputOf(
      Contextful.Fn<InputT, OutputT> fn) {
    return TypeDescriptors.extractFromTypeParameters(
        fn,
        Contextful.Fn.class,
        new TypeDescriptors.TypeVariableExtractor<Contextful.Fn<InputT, OutputT>, OutputT>() {});
  }
}
