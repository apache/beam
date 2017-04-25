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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;

/**
 * A utility class containing the Java primitives for
 * {@link TypeDescriptor} equivalents. Also, has methods
 * for classes that wrap Java primitives like {@link KV},
 * {@link Set}, {@link List}, and {@link Iterable}.
 */
public class TypeDescriptors {
  /**
   * The {@link TypeDescriptor} for Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Boolean&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Boolean
   */
  public static TypeDescriptor<Boolean> booleans() {
    return new TypeDescriptor<Boolean>() {};
  }

  /**
   * The {@link TypeDescriptor} for Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Double&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Double
   */
  public static TypeDescriptor<Double> doubles() {
    return new TypeDescriptor<Double>() {};
  }

  /**
   * The {@link TypeDescriptor} for Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Float&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Float
   */
  public static TypeDescriptor<Float> floats() {
    return new TypeDescriptor<Float>() {};
  }

  /**
   * The {@link TypeDescriptor} for Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Integer&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Integer
   */
  public static TypeDescriptor<Integer> integers() {
    return new TypeDescriptor<Integer>() {};
  }

  /**
   * The {@link TypeDescriptor} for Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Long&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Long
   */
  public static TypeDescriptor<Long> longs() {
    return new TypeDescriptor<Long>() {};
  }

  /**
   * The {@link TypeDescriptor} for Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Short&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Short
   */
  public static TypeDescriptor<Short> shorts() {
    return new TypeDescriptor<Short>() {};
  }

  /**
   * The {@link TypeDescriptor} for BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;BigDecimal&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for BigDecimal
   */
  public static TypeDescriptor<BigDecimal> bigdecimals() {
    return new TypeDescriptor<BigDecimal>() {};
  }

  /**
   * The {@link TypeDescriptor} for BigInteger.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;BigInteger&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for BigInteger
   */
  public static TypeDescriptor<BigInteger> bigintegers() {
    return new TypeDescriptor<BigInteger>() {};
  }

  /**
   * The {@link TypeDescriptor} for String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;String&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for String
   */
  public static TypeDescriptor<String> strings() {
    return new TypeDescriptor<String>() {};
  }

  /**
   * The {@link TypeDescriptor} for Character.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Character&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Character
   */
  public static TypeDescriptor<Character> characters() {
    return new TypeDescriptor<Character>() {};
  }

  /**
   * The {@link TypeDescriptor} for Byte.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Byte&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Byte
   */
  public static TypeDescriptor<Byte> bytes() {
    return new TypeDescriptor<Byte>() {};
  }

  /**
   * The {@link TypeDescriptor} for nulls/Void.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Void&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for nulls/Void
   */
  public static TypeDescriptor<Void> nulls() {
    return new TypeDescriptor<Void>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV}.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;K,V&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   * <pre>
   * {@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.kv(TypeDescriptors.strings(), TypeDescriptors.strings()))
   *         .via(...));
   * }
   * </pre>
   * @param key The {@link TypeDescriptor} for the key
   * @param value The {@link TypeDescriptor} for the value
   * @return A {@link TypeDescriptor} for {@link KV}
   */
  public static <K, V> TypeDescriptor<KV<K, V>>
    kvs(TypeDescriptor<K> key, TypeDescriptor<V> value) {
    TypeDescriptor<KV<K, V>> typeDescriptor =
        new TypeDescriptor<KV<K, V>>() {}
      .<K> where(new TypeParameter<K>() {}, key)
      .<V> where(new TypeParameter<V>() {}, value);

    return typeDescriptor;
  }

  /**
   * The {@link TypeDescriptor} for {@link Set}.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Set&lt;E&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   * <pre>
   * {@code
   * PCollection<String> words = ...;
   * PCollection<Set<String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.sets(TypeDescriptors.strings()))
   *         .via(...));
   * }
   * </pre>
   * @param element The {@link TypeDescriptor} for the set
   * @return A {@link TypeDescriptor} for {@link Set}
   */
  public static <T> TypeDescriptor<Set<T>>
    sets(TypeDescriptor<T> element) {
    TypeDescriptor<Set<T>> typeDescriptor =
        new TypeDescriptor<Set<T>>() {}
      .<T> where(new TypeParameter<T>() {}, element);

    return typeDescriptor;
  }

  /**
   * The {@link TypeDescriptor} for {@link List}.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;List&lt;E&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   * <pre>
   * {@code
   * PCollection<String> words = ...;
   * PCollection<List<String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.lists(TypeDescriptors.strings()))
   *         .via(...));
   * }
   * </pre>
   * @param element The {@link TypeDescriptor} for the list
   * @return A {@link TypeDescriptor} for {@link List}
   */
  public static <T> TypeDescriptor<List<T>>
    lists(TypeDescriptor<T> element) {
    TypeDescriptor<List<T>> typeDescriptor =
        new TypeDescriptor<List<T>>() {}
      .<T> where(new TypeParameter<T>() {}, element);

    return typeDescriptor;
  }

  /**
   * The {@link TypeDescriptor} for {@link Iterable}.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Iterable&lt;E&gt;&gt;() {};
   * </pre>
   *
   * <p>Example of use:
   * <pre>
   * {@code
   * PCollection<String> words = ...;
   * PCollection<Iterable<String>> words = words.apply(FlatMapElements
   *         .into(TypeDescriptors.iterables(TypeDescriptors.strings()))
   *         .via(...));
   * }
   * </pre>
   * @param iterable The {@link TypeDescriptor} for the iterable
   * @return A {@link TypeDescriptor} for {@link Iterable}
   */
  public static <T> TypeDescriptor<Iterable<T>>
    iterables(TypeDescriptor<T> iterable) {
    TypeDescriptor<Iterable<T>> typeDescriptor =
        new TypeDescriptor<Iterable<T>>() {}
      .<T> where(new TypeParameter<T>() {}, iterable);

    return typeDescriptor;
  }
}
