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
package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.in;
import static org.hamcrest.core.Is.is;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

/**
 * Static class for building and using {@link SerializableMatcher} instances.
 *
 * <p>Most matchers are wrappers for hamcrest's {@link Matchers}. Please be familiar with the
 * documentation there. Values retained by a {@link SerializableMatcher} are required to be
 * serializable, either via Java serialization or via a provided {@link Coder}.
 *
 * <p>The following matchers are novel to Apache Beam:
 *
 * <ul>
 *   <li>{@link #kvWithKey} for matching just the key of a {@link KV}.
 *   <li>{@link #kvWithValue} for matching just the value of a {@link KV}.
 *   <li>{@link #kv} for matching the key and value of a {@link KV}.
 * </ul>
 *
 * <p>For example, to match a group from {@link org.apache.beam.sdk.transforms.GroupByKey}, which
 * has type {@code KV<K, Iterable<V>>} for some {@code K} and {@code V} and where the order of the
 * iterable is undefined, use a matcher like {@code kv(equalTo("some key"), containsInAnyOrder(1, 2,
 * 3))}.
 */
public class SerializableMatchers implements Serializable {

  // Serializable only because of capture by anonymous inner classes
  private SerializableMatchers() {} // not instantiable

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#allOf(Iterable)}. */
  public static <T> SerializableMatcher<T> allOf(
      Iterable<SerializableMatcher<? super T>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Iterable<Matcher<? super T>> matchers = (Iterable) serializableMatchers;

    return fromSupplier(() -> Matchers.allOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#allOf(Matcher[])}. */
  @SafeVarargs
  public static <T> SerializableMatcher<T> allOf(final SerializableMatcher<T>... matchers) {
    return fromSupplier(() -> Matchers.allOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anyOf(Iterable)}. */
  public static <T> SerializableMatcher<T> anyOf(
      Iterable<SerializableMatcher<? super T>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Iterable<Matcher<? super T>> matchers = (Iterable) serializableMatchers;

    return fromSupplier(() -> Matchers.anyOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anyOf(Matcher[])}. */
  @SafeVarargs
  public static <T> SerializableMatcher<T> anyOf(final SerializableMatcher<T>... matchers) {
    return fromSupplier(() -> Matchers.anyOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anything()}. */
  public static SerializableMatcher<Object> anything() {
    return fromSupplier(Matchers::anything);
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<T[]> arrayContaining(
      final T... items) {
    return fromSupplier(() -> Matchers.arrayContaining(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<T[]> arrayContaining(Coder<T> coder, T... items) {

    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);

    return fromSupplier(() -> Matchers.arrayContaining(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<T[]> arrayContaining(
      final SerializableMatcher<? super T>... matchers) {
    return fromSupplier(() -> Matchers.arrayContaining(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(List)}.
   */
  public static <T> SerializableMatcher<T[]> arrayContaining(
      List<SerializableMatcher<? super T>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final List<Matcher<? super T>> matchers = (List) serializableMatchers;

    return fromSupplier(() -> Matchers.arrayContaining(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<T[]> arrayContainingInAnyOrder(
      final T... items) {

    return fromSupplier(() -> Matchers.arrayContainingInAnyOrder(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<T[]> arrayContainingInAnyOrder(Coder<T> coder, T... items) {

    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);

    return fromSupplier(() -> Matchers.arrayContaining(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<T[]> arrayContainingInAnyOrder(
      final SerializableMatcher<? super T>... matchers) {
    return fromSupplier(() -> Matchers.arrayContainingInAnyOrder(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Collection)}.
   */
  public static <T> SerializableMatcher<T[]> arrayContainingInAnyOrder(
      Collection<SerializableMatcher<? super T>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Collection<Matcher<? super T>> matchers = (Collection) serializableMatchers;

    return fromSupplier(() -> Matchers.arrayContainingInAnyOrder(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#arrayWithSize(int)}.
   */
  public static <T> SerializableMatcher<T[]> arrayWithSize(final int size) {
    return fromSupplier(() -> Matchers.arrayWithSize(size));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayWithSize(Matcher)}.
   */
  public static <T> SerializableMatcher<T[]> arrayWithSize(
      final SerializableMatcher<? super Integer> sizeMatcher) {
    return fromSupplier(() -> Matchers.arrayWithSize(sizeMatcher));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#closeTo(double,double)}.
   */
  public static SerializableMatcher<Double> closeTo(final double target, final double error) {
    return fromSupplier(() -> Matchers.closeTo(target, error));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<Iterable<? extends T>> contains(
      final T... items) {
    return fromSupplier(() -> Matchers.contains(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<? extends T>> contains(
      Coder<T> coder, T... items) {

    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);

    return fromSupplier(() -> Matchers.containsInAnyOrder(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<? extends T>> contains(
      final SerializableMatcher<? super T>... matchers) {
    return fromSupplier(() -> Matchers.contains(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(List)}. */
  public static <T extends Serializable> SerializableMatcher<Iterable<? extends T>> contains(
      List<SerializableMatcher<? super T>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final List<Matcher<? super T>> matchers = (List) serializableMatchers;

    return fromSupplier(() -> Matchers.contains(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable>
      SerializableMatcher<Iterable<? extends T>> containsInAnyOrder(final T... items) {
    return fromSupplier(() -> Matchers.containsInAnyOrder(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<? extends T>> containsInAnyOrder(
      Coder<T> coder, T... items) {

    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);

    return fromSupplier(() -> Matchers.containsInAnyOrder(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<? extends T>> containsInAnyOrder(
      final SerializableMatcher<? super T>... matchers) {
    return fromSupplier(() -> Matchers.containsInAnyOrder(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Collection)}.
   */
  public static <T> SerializableMatcher<Iterable<? extends T>> containsInAnyOrder(
      Collection<SerializableMatcher<? super T>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Collection<Matcher<? super T>> matchers = (Collection) serializableMatchers;

    return fromSupplier(() -> Matchers.containsInAnyOrder(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#containsString}. */
  public static SerializableMatcher<String> containsString(final String substring) {
    return fromSupplier(() -> Matchers.containsString(substring));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#empty()}. */
  public static <T> SerializableMatcher<Collection<? extends T>> empty() {
    return fromSupplier(Matchers::empty);
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#emptyArray()}. */
  public static <T> SerializableMatcher<T[]> emptyArray() {
    return fromSupplier(Matchers::emptyArray);
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#emptyIterable()}. */
  public static <T> SerializableMatcher<Iterable<? extends T>> emptyIterable() {
    return fromSupplier(Matchers::emptyIterable);
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#endsWith}. */
  public static SerializableMatcher<String> endsWith(final String substring) {
    return fromSupplier(() -> Matchers.endsWith(substring));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#equalTo(Object)}. */
  public static <T extends Serializable> SerializableMatcher<T> equalTo(final T expected) {
    return fromSupplier(() -> Matchers.equalTo(expected));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#equalTo(Object)}.
   *
   * <p>The expected value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<T> equalTo(Coder<T> coder, T expected) {

    final SerializableSupplier<T> expectedSupplier = new SerializableViaCoder<>(coder, expected);

    return fromSupplier(() -> Matchers.equalTo(expectedSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThan(Comparable)}.
   */
  public static <T extends Comparable<T> & Serializable> SerializableMatcher<T> greaterThan(
      final T target) {
    return fromSupplier(() -> Matchers.greaterThan(target));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThan(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T> & Serializable> SerializableMatcher<T> greaterThan(
      final Coder<T> coder, T target) {
    final SerializableSupplier<T> targetSupplier = new SerializableViaCoder<>(coder, target);
    return fromSupplier(() -> Matchers.greaterThan(targetSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThanOrEqualTo(Comparable)}.
   */
  public static <T extends Comparable<T>> SerializableMatcher<T> greaterThanOrEqualTo(
      final T target) {
    return fromSupplier(() -> Matchers.greaterThanOrEqualTo(target));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThanOrEqualTo(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T> & Serializable>
      SerializableMatcher<T> greaterThanOrEqualTo(final Coder<T> coder, T target) {
    final SerializableSupplier<T> targetSupplier = new SerializableViaCoder<>(coder, target);
    return fromSupplier(() -> Matchers.greaterThanOrEqualTo(targetSupplier.get()));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasItem(Object)}. */
  public static <T extends Serializable> SerializableMatcher<Iterable<? super T>> hasItem(
      final T target) {
    return fromSupplier(() -> Matchers.hasItem(target));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasItem(Object)}.
   *
   * <p>The item of type {@code T} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<Iterable<? super T>> hasItem(Coder<T> coder, T target) {
    final SerializableSupplier<T> targetSupplier = new SerializableViaCoder<>(coder, target);
    return fromSupplier(() -> Matchers.hasItem(targetSupplier.get()));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasItem(Matcher)}. */
  public static <T> SerializableMatcher<Iterable<? super T>> hasItem(
      final SerializableMatcher<? super T> matcher) {
    return fromSupplier(() -> Matchers.hasItem(matcher));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasSize(int)}. */
  public static <T> SerializableMatcher<Collection<? extends T>> hasSize(final int size) {
    return fromSupplier(() -> Matchers.hasSize(size));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasSize(Matcher)}. */
  public static <T> SerializableMatcher<Collection<? extends T>> hasSize(
      final SerializableMatcher<? super Integer> sizeMatcher) {
    return fromSupplier(() -> Matchers.hasSize(sizeMatcher));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#iterableWithSize(int)}.
   */
  public static <T> SerializableMatcher<Iterable<T>> iterableWithSize(final int size) {
    return fromSupplier(() -> Matchers.iterableWithSize(size));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#iterableWithSize(Matcher)}.
   */
  public static <T> SerializableMatcher<Iterable<T>> iterableWithSize(
      final SerializableMatcher<? super Integer> sizeMatcher) {
    return fromSupplier(() -> Matchers.iterableWithSize(sizeMatcher));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Collection)}. */
  public static <T extends Serializable> SerializableMatcher<T> isIn(
      final Collection<T> collection) {
    return fromSupplier(() -> is(in(collection)));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Collection)}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<T> isIn(Coder<T> coder, Collection<T> collection) {
    @SuppressWarnings("unchecked")
    T[] items = (T[]) collection.toArray();
    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);
    return fromSupplier(() -> is(in(itemsSupplier.get())));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Object[])}. */
  public static <T extends Serializable> SerializableMatcher<T> isIn(final T[] items) {
    return fromSupplier(() -> is(in(items)));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<T> isIn(Coder<T> coder, T[] items) {
    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);
    return fromSupplier(() -> is(in(itemsSupplier.get())));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#isOneOf}. */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<T> isOneOf(final T... elems) {
    return fromSupplier(() -> Matchers.isOneOf(elems));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#isOneOf}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<T> isOneOf(Coder<T> coder, T... items) {
    final SerializableSupplier<T[]> itemsSupplier = new SerializableArrayViaCoder<>(coder, items);
    return fromSupplier(() -> Matchers.isOneOf(itemsSupplier.get()));
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with the specified key. */
  public static <K extends Serializable, V>
      SerializableMatcher<KV<? extends K, ? extends V>> kvWithKey(K key) {
    return new KvKeyMatcher<>(equalTo(key));
  }

  /**
   * A {@link SerializableMatcher} that matches any {@link KV} with the specified key.
   *
   * <p>The key of type {@code K} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <K, V> SerializableMatcher<KV<? extends K, ? extends V>> kvWithKey(
      Coder<K> coder, K key) {
    return new KvKeyMatcher<>(equalTo(coder, key));
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with matching key. */
  public static <K, V> SerializableMatcher<KV<? extends K, ? extends V>> kvWithKey(
      final SerializableMatcher<? super K> keyMatcher) {
    return new KvKeyMatcher<>(keyMatcher);
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with the specified value. */
  public static <K, V extends Serializable>
      SerializableMatcher<KV<? extends K, ? extends V>> kvWithValue(V value) {
    return new KvValueMatcher<>(equalTo(value));
  }

  /**
   * A {@link SerializableMatcher} that matches any {@link KV} with the specified value.
   *
   * <p>The value of type {@code V} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <K, V> SerializableMatcher<KV<? extends K, ? extends V>> kvWithValue(
      Coder<V> coder, V value) {
    return new KvValueMatcher<>(equalTo(coder, value));
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with matching value. */
  public static <K, V> SerializableMatcher<KV<? extends K, ? extends V>> kvWithValue(
      final SerializableMatcher<? super V> valueMatcher) {
    return new KvValueMatcher<>(valueMatcher);
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with matching key and value. */
  public static <K, V> SerializableMatcher<KV<? extends K, ? extends V>> kv(
      final SerializableMatcher<? super K> keyMatcher,
      final SerializableMatcher<? super V> valueMatcher) {

    return SerializableMatchers.allOf(
        SerializableMatchers.kvWithKey(keyMatcher), SerializableMatchers.kvWithValue(valueMatcher));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#lessThan(Comparable)}.
   */
  public static <T extends Comparable<T> & Serializable> SerializableMatcher<T> lessThan(
      final T target) {
    return fromSupplier(() -> Matchers.lessThan(target));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#lessThan(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T>> SerializableMatcher<T> lessThan(
      Coder<T> coder, T target) {
    final SerializableSupplier<T> targetSupplier = new SerializableViaCoder<>(coder, target);
    return fromSupplier(() -> Matchers.lessThan(targetSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#lessThanOrEqualTo(Comparable)}.
   */
  public static <T extends Comparable<T> & Serializable> SerializableMatcher<T> lessThanOrEqualTo(
      final T target) {
    return fromSupplier(() -> Matchers.lessThanOrEqualTo(target));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#lessThanOrEqualTo(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T>> SerializableMatcher<T> lessThanOrEqualTo(
      Coder<T> coder, T target) {
    final SerializableSupplier<T> targetSupplier = new SerializableViaCoder<>(coder, target);
    return fromSupplier(() -> Matchers.lessThanOrEqualTo(targetSupplier.get()));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#not}. */
  public static <T> SerializableMatcher<T> not(final SerializableMatcher<T> matcher) {
    return fromSupplier(() -> Matchers.not(matcher));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#nullValue}. */
  public static SerializableMatcher<Object> nullValue() {
    return fromSupplier(Matchers::nullValue);
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#startsWith}. */
  public static SerializableMatcher<String> startsWith(final String substring) {
    return fromSupplier(() -> Matchers.startsWith(substring));
  }

  private static class KvKeyMatcher<K, V> extends BaseMatcher<KV<? extends K, ? extends V>>
      implements SerializableMatcher<KV<? extends K, ? extends V>> {
    private final SerializableMatcher<? super K> keyMatcher;

    public KvKeyMatcher(SerializableMatcher<? super K> keyMatcher) {
      this.keyMatcher = keyMatcher;
    }

    @Override
    public boolean matches(Object item) {
      @SuppressWarnings("unchecked")
      KV<K, ?> kvItem = (KV<K, ?>) item;
      return keyMatcher.matches(kvItem.getKey());
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription) {
      @SuppressWarnings("unchecked")
      KV<K, ?> kvItem = (KV<K, ?>) item;
      if (!keyMatcher.matches(kvItem.getKey())) {
        mismatchDescription.appendText("key did not match: ");
        keyMatcher.describeMismatch(kvItem.getKey(), mismatchDescription);
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("KV with key matching ");
      keyMatcher.describeTo(description);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).addValue(keyMatcher).toString();
    }
  }

  private static class KvValueMatcher<K, V> extends BaseMatcher<KV<? extends K, ? extends V>>
      implements SerializableMatcher<KV<? extends K, ? extends V>> {
    private final SerializableMatcher<? super V> valueMatcher;

    public KvValueMatcher(SerializableMatcher<? super V> valueMatcher) {
      this.valueMatcher = valueMatcher;
    }

    @Override
    public boolean matches(Object item) {
      @SuppressWarnings("unchecked")
      KV<?, V> kvItem = (KV<?, V>) item;
      return valueMatcher.matches(kvItem.getValue());
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription) {
      @SuppressWarnings("unchecked")
      KV<?, V> kvItem = (KV<?, V>) item;
      if (!valueMatcher.matches(kvItem.getValue())) {
        mismatchDescription.appendText("value did not match: ");
        valueMatcher.describeMismatch(kvItem.getValue(), mismatchDescription);
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("KV with value matching ");
      valueMatcher.describeTo(description);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).addValue(valueMatcher).toString();
    }
  }

  /**
   * Constructs a {@link SerializableMatcher} from a non-serializable {@link Matcher} via
   * indirection through {@link SerializableSupplier}.
   *
   * <p>To wrap a {@link Matcher} which is not serializable, provide a {@link SerializableSupplier}
   * with a {@link SerializableSupplier#get()} method that returns a fresh instance of the {@link
   * Matcher} desired. The resulting {@link SerializableMatcher} will behave according to the {@link
   * Matcher} returned by {@link SerializableSupplier#get() get()} when it is invoked during
   * matching (which may occur on another machine).
   *
   * <pre>{@code
   * return fromSupplier(new SerializableSupplier<Matcher<T>>() {
   *   *     @Override
   *     public Matcher<T> get() {
   *       return new MyMatcherForT();
   *     }
   * });
   * }</pre>
   */
  public static <T> SerializableMatcher<T> fromSupplier(SerializableSupplier<Matcher<T>> supplier) {
    return new SerializableMatcherFromSupplier<>(supplier);
  }

  /**
   * Supplies values of type {@code T}, and is serializable. Thus, even if {@code T} is not
   * serializable, the supplier can be serialized and provide a {@code T} wherever it is
   * deserialized.
   *
   * @param <T> the type of value supplied.
   */
  public interface SerializableSupplier<T> extends Serializable {
    T get();
  }

  /**
   * Since the delegate {@link Matcher} is not generally serializable, instead this takes a nullary
   * SerializableFunction to return such a matcher.
   */
  private static class SerializableMatcherFromSupplier<T> extends BaseMatcher<T>
      implements SerializableMatcher<T> {

    private SerializableSupplier<Matcher<T>> supplier;

    public SerializableMatcherFromSupplier(SerializableSupplier<Matcher<T>> supplier) {
      this.supplier = supplier;
    }

    @Override
    public void describeTo(Description description) {
      supplier.get().describeTo(description);
    }

    @Override
    public boolean matches(Object item) {
      return supplier.get().matches(item);
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription) {
      supplier.get().describeMismatch(item, mismatchDescription);
    }
  }

  /**
   * Wraps any value that can be encoded via a {@link Coder} to make it {@link Serializable}. This
   * is not likely to be a good encoding, so should be used only for tests, where data volume is
   * small and minor costs are not critical.
   */
  private static class SerializableViaCoder<T> implements SerializableSupplier<T> {
    /** Cached value that is not serialized. */
    private transient @Nullable T value;

    /** The bytes of {@link #value} when encoded via {@link #coder}. */
    private byte[] encodedValue;

    private Coder<T> coder;

    public SerializableViaCoder(Coder<T> coder, T value) {
      this.coder = coder;
      this.value = value;
      try {
        this.encodedValue = CoderUtils.encodeToByteArray(coder, value);
      } catch (CoderException exc) {
        throw new RuntimeException("Error serializing via Coder", exc);
      }
    }

    @Override
    public T get() {
      if (value == null) {
        try {
          value = CoderUtils.decodeFromByteArray(coder, encodedValue);
        } catch (CoderException exc) {
          throw new RuntimeException("Error deserializing via Coder", exc);
        }
      }
      return value;
    }
  }

  /**
   * Wraps any array with values that can be encoded via a {@link Coder} to make it {@link
   * Serializable}. This is not likely to be a good encoding, so should be used only for tests,
   * where data volume is small and minor costs are not critical.
   */
  private static class SerializableArrayViaCoder<T> implements SerializableSupplier<T[]> {
    /** Cached value that is not serialized. */
    private transient T @Nullable [] value;

    /** The bytes of {@link #value} when encoded via {@link #coder}. */
    private byte[] encodedValue;

    private Coder<List<T>> coder;

    public SerializableArrayViaCoder(Coder<T> elementCoder, T[] value) {
      this.coder = ListCoder.of(elementCoder);
      this.value = value;
      try {
        this.encodedValue = CoderUtils.encodeToByteArray(coder, Arrays.asList(value));
      } catch (CoderException exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public T[] get() {
      if (value == null) {
        try {
          @SuppressWarnings("unchecked")
          T[] decoded = (T[]) CoderUtils.decodeFromByteArray(coder, encodedValue).toArray();
          value = decoded;
        } catch (CoderException exc) {
          throw new RuntimeException("Error deserializing via Coder", exc);
        }
      }
      return value;
    }
  }
}
