/*
 * Copyright (C) 2016 Google Inc.
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

package com.google.cloud.dataflow.contrib.sorter;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/** Tests for {@link SortValues} transform. */
@RunWith(JUnit4.class)
public class SortValuesTest {

  @Test
  public void testSecondaryKeySorting() throws Exception {
    Pipeline p = TestPipeline.create();

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
        input.apply(GroupByKey.<String, KV<String, Integer>>create());

    // For every Key, sort the iterable of <SecondaryKey, Value> pairs by SecondaryKey.
    PCollection<KV<String, Iterable<KV<String, Integer>>>> groupedAndSorted =
        grouped.apply(
            SortValues.<String, String, Integer>create(new BufferedExternalSorter.Options()));

    DataflowAssert.that(groupedAndSorted)
        .satisfies(new AssertThatHasExpectedContentsForTestSecondaryKeySorting());

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestSecondaryKeySorting
      implements SerializableFunction<Iterable<KV<String, Iterable<KV<String, Integer>>>>, Void> {
    @SuppressWarnings("unchecked")
    @Override
    public Void apply(Iterable<KV<String, Iterable<KV<String, Integer>>>> actual) {
      assertThat(
          actual,
          containsInAnyOrder(
              KvMatcher.isKv(
                  is("key1"),
                  contains(
                      KvMatcher.isKv(is("secondaryKey1"), is(10)),
                      KvMatcher.isKv(is("secondaryKey2"), is(20)),
                      KvMatcher.isKv(is("secondaryKey3"), is(30)))),
              KvMatcher.isKv(
                  is("key2"),
                  contains(
                      KvMatcher.isKv(is("secondaryKey1"), is(100)),
                      KvMatcher.isKv(is("secondaryKey2"), is(200))))));
      return null;
    }
  }

  /** Matcher for KVs. Forked from Beam's com.google.cloud.dataflow/sdk/TestUtils.java */
  public static class KvMatcher<K, V> extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
    final Matcher<? super K> keyMatcher;
    final Matcher<? super V> valueMatcher;

    public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
      return new KvMatcher<>(keyMatcher, valueMatcher);
    }

    public KvMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
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
