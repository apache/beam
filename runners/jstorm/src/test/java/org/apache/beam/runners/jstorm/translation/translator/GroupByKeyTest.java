
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
package org.apache.beam.runners.jstorm.translation.translator;

import org.apache.beam.runners.jstorm.StormPipelineOptions;

import org.apache.beam.runners.jstorm.StormRunner;
import org.apache.beam.runners.jstorm.TestJStormRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link GroupByKey} with {@link StormRunner}.
 */
@RunWith(JUnit4.class)
public class GroupByKeyTest {

    static final String[] WORDS_ARRAY = new String[] {
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", "" };

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    @Test
    public void testGroupByKey() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline p = Pipeline.create(options);

        List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
                KV.of("k1", 3),
                KV.of("k5", Integer.MAX_VALUE),
                KV.of("k5", Integer.MIN_VALUE),
                KV.of("k2", 66),
                KV.of("k1", 4),
                KV.of("k2", -33),
                KV.of("k3", 0));

        PCollection<KV<String, Integer>> input =
                p.apply(Create.of(ungroupedPairs)
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

        PCollection<KV<String, Iterable<Integer>>> output =
                input.apply(GroupByKey.<String, Integer>create());

        PAssert.that(output)
                .satisfies(new AssertThatHasExpectedContentsForTestGroupByKey());

        p.run();
    }

    @Test
    public void testCountGloballyBasic() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.apply(Create.of(WORDS));

        PCollection<Long> output =
                input.apply(Count.<String>globally());

        PAssert.that(output)
                .containsInAnyOrder(13L);
        p.run();
    }

    static class AssertThatHasExpectedContentsForTestGroupByKey
            implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>,
            Void> {
        @Override
        public Void apply(Iterable<KV<String, Iterable<Integer>>> actual) {
            assertThat(actual, containsInAnyOrder(
                    KvMatcher.isKv(is("k1"), containsInAnyOrder(3, 4)),
                    KvMatcher.isKv(is("k5"), containsInAnyOrder(Integer.MAX_VALUE,
                            Integer.MIN_VALUE)),
                    KvMatcher.isKv(is("k2"), containsInAnyOrder(66, -33)),
                    KvMatcher.isKv(is("k3"), containsInAnyOrder(0))));
            return null;
        }
    }

    /**
     * Matcher for KVs.
     */
    public static class KvMatcher<K, V>
            extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
        final Matcher<? super K> keyMatcher;
        final Matcher<? super V> valueMatcher;

        public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher,
                                                  Matcher<V> valueMatcher) {
            return new KvMatcher<>(keyMatcher, valueMatcher);
        }

        public KvMatcher(Matcher<? super K> keyMatcher,
                         Matcher<? super V> valueMatcher) {
            this.keyMatcher = keyMatcher;
            this.valueMatcher = valueMatcher;
        }

        @Override
        public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
            return keyMatcher.matches(kv.getKey())
                    && valueMatcher.matches(kv.getValue());
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("a KV(").appendValue(keyMatcher)
                    .appendText(", ").appendValue(valueMatcher)
                    .appendText(")");
        }
    }
}
