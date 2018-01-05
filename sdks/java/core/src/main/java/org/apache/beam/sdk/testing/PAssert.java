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

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;

/**
 * An assertion on the contents of a {@link PCollection} incorporated into the pipeline. Such an
 * assertion can be checked no matter what kind of {@link PipelineRunner} is used.
 *
 * <p>Note that the {@code PAssert} call must precede the call to {@link Pipeline#run}.
 *
 * <p>Examples of use: <pre>{@code
 * Pipeline p = TestPipeline.create();
 * ...
 * PCollection<String> output =
 *      input
 *      .apply(ParDo.of(new TestDoFn()));
 * PAssert.that(output)
 *     .containsInAnyOrder("out1", "out2", "out3");
 * ...
 * PCollection<Integer> ints = ...
 * PCollection<Integer> sum =
 *     ints
 *     .apply(Combine.globally(new SumInts()));
 * PAssert.that(sum)
 *     .is(42);
 * ...
 * p.run();
 * }</pre>
 *
 * <p>JUnit and Hamcrest must be linked in by any code that uses PAssert.
 */
public class PAssert {
  static final PAsserts.AssertBase ASSERTS = new PAsserts.AssertBase<Matcher<?>>() {
    @Override
    public <T> void assertThat(T actual, Matcher<?> matcher) {
      Assert.assertThat(actual, (Matcher<? super T>) matcher);
    }

    @Override
    public <T> void assertEquals(T actual, T expected) {
      Assert.assertEquals(actual, expected);
    }

    @Override
    public <T> void assertNotEquals(T actual, T expected) {
      Assert.assertNotEquals(actual, expected);
    }

    @Override
    public <T> Matcher<Iterable<? extends T>> containsInAnyOrder(final T[] expected) {
      return Matchers.containsInAnyOrder(expected);
    }
  };

  // Do not instantiate.
  private PAssert() {}

  /**
   * Constructs an {@link PAsserts.IterableAssert} for the elements of
   * the provided {@link PCollection}.
   */
  public static <T> PAsserts.IterableAssert<T> that(PCollection<T> actual) {
    return that(actual.getName(), actual);
  }

  /**
   * Constructs an {@link PAsserts.IterableAssert} for the elements of
   * the provided {@link PCollection}
   * with the specified reason.
   */
  public static <T> PAsserts.IterableAssert<T> that(String reason, PCollection<T> actual) {
    return PAsserts.that(reason, actual, ASSERTS);
  }

  /**
   * Constructs an {@link PAsserts.IterableAssert} for the value of
   * the provided {@link PCollection} which
   * must contain a single {@code Iterable<T>} value.
   */
  public static <T> PAsserts.IterableAssert<T> thatSingletonIterable(
      PCollection<? extends Iterable<T>> actual) {
    return thatSingletonIterable(actual.getName(), actual);
  }

  /**
   * Constructs an {@link PAsserts.IterableAssert} for the value of
   * the provided {@link PCollection } with
   * the specified reason. The provided PCollection must contain a single
   * {@code Iterable<T>} value.
   */
  public static <T> PAsserts.IterableAssert<T> thatSingletonIterable(
      String reason, PCollection<? extends Iterable<T>> actual) {
    return PAsserts.thatSingletonIterable(reason, actual, ASSERTS);
  }

  /**
   * Constructs a {@link PAsserts.SingletonAssert} for the value of the provided
   * {@code PCollection PCollection<T>}, which must be a singleton.
   */
  public static <T> PAsserts.SingletonAssert<T> thatSingleton(PCollection<T> actual) {
    return thatSingleton(actual.getName(), actual);
  }

  /**
   * Constructs a {@link PAsserts.SingletonAssert} for the value of the provided
   * {@code PCollection PCollection<T>} with the specified reason. The provided PCollection must be
   * a singleton.
   */
  public static <T> PAsserts.SingletonAssert<T> thatSingleton(String reason,
                                                              PCollection<T> actual) {
    return PAsserts.thatSingleton(reason, actual, ASSERTS);
  }

  /**
   * Constructs a {@link PAsserts.SingletonAssert} for the value of
   * the provided {@link PCollection}.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any
   * {@code Coder<K, V>}.
   */
  public static <K, V> PAsserts.SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
      PCollection<KV<K, V>> actual) {
    return thatMultimap(actual.getName(), actual);
  }

  /**
   * Constructs a {@link PAsserts.SingletonAssert} for the value of
   * the provided {@link PCollection} with the
   * specified reason.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any
   * {@code Coder<K, V>}.
   */
  public static <K, V> PAsserts.SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
      String reason, PCollection<KV<K, V>> actual) {
    return PAsserts.thatMultimap(reason, actual, ASSERTS);
  }

  /**
   * Constructs a {@link PAsserts.SingletonAssert} for the value of
   * the provided {@link PCollection}, which
   * must have at most one value per key.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any
   * {@code Coder<K, V>}.
   */
  public static <K, V> PAsserts.SingletonAssert<Map<K, V>> thatMap(PCollection<KV<K, V>> actual) {
    return thatMap(actual.getName(), actual);
  }

  /**
   * Constructs a {@link PAsserts.SingletonAssert} for the value of
   * the provided {@link PCollection} with
   * the specified reason. The {@link PCollection} must have at most one value per key.
   *
   * <p>Note that the actual value must be coded by a {@link KvCoder}, not just any
   * {@code Coder<K, V>}.
   */
  public static <K, V> PAsserts.SingletonAssert<Map<K, V>> thatMap(
      String reason, PCollection<KV<K, V>> actual) {
    return PAsserts.thatMap(reason, actual, ASSERTS);
  }

  /**
   * @deprecated use PAsserts flavor.
   *
   * @param pipeline the pipeline.
   * @return the number of asserts.
   */
  @Deprecated
  public static int countAsserts(Pipeline pipeline) {
    return PAsserts.countAsserts(pipeline);
  }
}
