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
package org.apache.beam.sdk.extensions.sorter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SortValues} transform. */
@RunWith(JUnit4.class)
public class SortValuesTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testSecondaryKeySorting() {
    // Create a PCollection of <Key, <SecondaryKey, Value>> pairs.
    PCollection<KV<String, KV<String, Integer>>> input =
        p.apply(
            Create.of(
                Arrays.asList(
                    KV.of("key1", KV.of("secondaryKey2", 20)),
                    KV.of("key2", KV.of("secondaryKey2", 200)),
                    KV.of("key1", KV.of("secondaryKey3", 30)),
                    KV.of("key1", KV.of("secondaryKey1", 10)),
                    KV.of("key2", KV.of("secondaryKey1", 100)))));

    // Group by Key, bringing <SecondaryKey, Value> pairs for the same Key together.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> grouped =
        input.apply(GroupByKey.create());

    // For every Key, sort the iterable of <SecondaryKey, Value> pairs by SecondaryKey.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedAndSorted =
        grouped.apply(SortValues.create(BufferedExternalSorter.options()));

    PAssert.that(groupedAndSorted)
        .satisfies(
            new AssertThatHasExpectedContentsForTestSecondaryKeySorting<>(
                Arrays.asList(
                    KV.of(
                        "key1",
                        Arrays.asList(
                            KV.of("secondaryKey1", 10),
                            KV.of("secondaryKey2", 20),
                            KV.of("secondaryKey3", 30))),
                    KV.of(
                        "key2",
                        Arrays.asList(KV.of("secondaryKey1", 100), KV.of("secondaryKey2", 200))))));

    p.run();
  }

  @Test
  public void testSecondaryKeyByteOptimization() {
    PCollection<KV<String, KV<byte[], Integer>>> input =
        p.apply(
            Create.of(
                Arrays.asList(
                    KV.of("key1", KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), 20)),
                    KV.of("key2", KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), 200)),
                    KV.of("key1", KV.of("secondaryKey3".getBytes(StandardCharsets.UTF_8), 30)),
                    KV.of("key1", KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), 10)),
                    KV.of("key2", KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), 100)))));

    // Group by Key, bringing <SecondaryKey, Value> pairs for the same Key together.
    PCollection<KV<String, Iterable<KV<byte[], Integer>>>> grouped =
        input.apply(GroupByKey.create());

    // For every Key, sort the iterable of <SecondaryKey, Value> pairs by SecondaryKey.
    PCollection<KV<String, Iterable<KV<byte[], Integer>>>> groupedAndSorted =
        grouped.apply(SortValues.create(BufferedExternalSorter.options()));

    PAssert.that(groupedAndSorted)
        .satisfies(
            new AssertThatHasExpectedContentsForTestSecondaryKeySorting<>(
                Arrays.asList(
                    KV.of(
                        "key1",
                        Arrays.asList(
                            KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), 10),
                            KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), 20),
                            KV.of("secondaryKey3".getBytes(StandardCharsets.UTF_8), 30))),
                    KV.of(
                        "key2",
                        Arrays.asList(
                            KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), 100),
                            KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), 200))))));

    p.run();
  }

  @Test
  public void testSecondaryKeyAndValueByteOptimization() {
    PCollection<KV<String, KV<byte[], byte[]>>> input =
        p.apply(
            Create.of(
                Arrays.asList(
                    KV.of(
                        "key1",
                        KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), new byte[] {1})),
                    KV.of(
                        "key2",
                        KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), new byte[] {2})),
                    KV.of(
                        "key1",
                        KV.of("secondaryKey3".getBytes(StandardCharsets.UTF_8), new byte[] {3})),
                    KV.of(
                        "key1",
                        KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), new byte[] {4})),
                    KV.of(
                        "key2",
                        KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), new byte[] {5})))));

    // Group by Key, bringing <SecondaryKey, Value> pairs for the same Key together.
    PCollection<KV<String, Iterable<KV<byte[], byte[]>>>> grouped =
        input.apply(GroupByKey.create());

    // For every Key, sort the iterable of <SecondaryKey, Value> pairs by SecondaryKey.
    PCollection<KV<String, Iterable<KV<byte[], byte[]>>>> groupedAndSorted =
        grouped.apply(SortValues.create(BufferedExternalSorter.options()));

    PAssert.that(groupedAndSorted)
        .satisfies(
            new AssertThatHasExpectedContentsForTestSecondaryKeySorting<>(
                Arrays.asList(
                    KV.of(
                        "key1",
                        Arrays.asList(
                            KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), new byte[] {4}),
                            KV.of("secondaryKey2".getBytes(StandardCharsets.UTF_8), new byte[] {1}),
                            KV.of(
                                "secondaryKey3".getBytes(StandardCharsets.UTF_8), new byte[] {3}))),
                    KV.of(
                        "key2",
                        Arrays.asList(
                            KV.of("secondaryKey1".getBytes(StandardCharsets.UTF_8), new byte[] {5}),
                            KV.of(
                                "secondaryKey2".getBytes(StandardCharsets.UTF_8),
                                new byte[] {2}))))));

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestSecondaryKeySorting<SecondaryKeyT, ValueT>
      implements SerializableFunction<
          Iterable<KV<String, Iterable<KV<SecondaryKeyT, ValueT>>>>, Void> {
    final List<KV<String, List<KV<SecondaryKeyT, ValueT>>>> expected;

    AssertThatHasExpectedContentsForTestSecondaryKeySorting(
        List<KV<String, List<KV<SecondaryKeyT, ValueT>>>> expected) {
      this.expected = expected;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Void apply(Iterable<KV<String, Iterable<KV<SecondaryKeyT, ValueT>>>> actual) {
      assertThat(
          actual,
          containsInAnyOrder(
              expected.stream()
                  .map(
                      kv1 ->
                          KvMatcher.isKv(
                              is(kv1.getKey()),
                              contains(
                                  kv1.getValue().stream()
                                      .map(
                                          kv2 ->
                                              KvMatcher.isKv(is(kv2.getKey()), is(kv2.getValue())))
                                      .collect(Collectors.toList()))))
                  .collect(Collectors.toList())));
      return null;
    }
  }

  /** Matcher for KVs. Forked from Beam's org/apache/beam/sdk/TestUtils.java */
  static class KvMatcher<K extends @Nullable Object, V extends @Nullable Object>
      extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
    final Matcher<? super K> keyMatcher;
    final Matcher<? super V> valueMatcher;

    static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
      return new KvMatcher<>(keyMatcher, valueMatcher);
    }

    KvMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
    @SuppressWarnings("nullness") // org.hamcrest.Matcher does not have precise types
    public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
      return keyMatcher.matches(kv.getKey()) && valueMatcher.matches(kv.getValue());
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a KV(")
          .appendValue(keyMatcher)
          .appendText(", ")
          .appendValue(valueMatcher)
          .appendText(")");
    }
  }
}
