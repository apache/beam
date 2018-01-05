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
package org.apache.beam.sdk.testing.junit5;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.testing.PAsserts;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;

/**
 * JUnit 5 flavor of PAssert. See JUnit 4 version for more details.
 *nop IMPORTANT: you must have hamcrest in the classpath for now.
 */
public final class PAssert {
    private static final PAsserts.AssertBase ASSERTS =
            new PAsserts.AssertBase<Function<Object, Boolean>>() {
                @Override
                public <T> void assertThat(T actual, Function<Object, Boolean> booleanFunction) {
                    Assertions.assertTrue(
                            booleanFunction.apply(actual),
                            "condition on " + actual + " failed");
                }

                @Override
                public <T> void assertEquals(T actual, T expected) {
                    Assertions.assertEquals(actual, expected);
                }

                @Override
                public <T> void assertNotEquals(T actual, T expected) {
                    Assertions.assertNotEquals(actual, expected);
                }

                @Override
                public <T> Function<Object, Boolean> containsInAnyOrder(final T[] expected) {
                    return value -> Stream.of(expected)
                            .noneMatch(i -> StreamSupport.stream(
                                    Iterable.class.cast(value).spliterator(), false)
                                    .anyMatch(v -> i == v || (i != null && i.equals(v))));
                }
            };

    private PAssert() {
        // no-op
    }

    public static <T> PAsserts.IterableAssert<T> that(PCollection<T> actual) {
        return that(actual.getName(), actual);
    }

    public static <T> PAsserts.IterableAssert<T> that(String reason, PCollection<T> actual) {
        return PAsserts.that(reason, actual, ASSERTS);
    }

    public static <T> PAsserts.IterableAssert<T> thatSingletonIterable(
            PCollection<? extends Iterable<T>> actual) {
        return thatSingletonIterable(actual.getName(), actual);
    }

    public static <T> PAsserts.IterableAssert<T> thatSingletonIterable(
            String reason, PCollection<? extends Iterable<T>> actual) {
        return PAsserts.thatSingletonIterable(reason, actual, ASSERTS);
    }

    public static <T> PAsserts.SingletonAssert<T> thatSingleton(PCollection<T> actual) {
        return thatSingleton(actual.getName(), actual);
    }

    public static <T> PAsserts.SingletonAssert<T> thatSingleton(String reason,
                                                                PCollection<T> actual) {
        return PAsserts.thatSingleton(reason, actual, ASSERTS);
    }

    public static <K, V> PAsserts.SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
            PCollection<KV<K, V>> actual) {
        return thatMultimap(actual.getName(), actual);
    }

    public static <K, V> PAsserts.SingletonAssert<Map<K, Iterable<V>>> thatMultimap(
            String reason, PCollection<KV<K, V>> actual) {
        return PAsserts.thatMultimap(reason, actual, ASSERTS);
    }

    public static <K, V> PAsserts.SingletonAssert<Map<K, V>> thatMap(PCollection<KV<K, V>> actual) {
        return thatMap(actual.getName(), actual);
    }

    public static <K, V> PAsserts.SingletonAssert<Map<K, V>> thatMap(
            String reason, PCollection<KV<K, V>> actual) {
        return PAsserts.thatMap(reason, actual, ASSERTS);
    }
}
