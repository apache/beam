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
package org.apache.beam.sdk.fn.stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.fn.stream.PrefetchableIterables.Default;
import org.apache.beam.sdk.fn.stream.PrefetchableIteratorsTest.ReadyAfterPrefetchUntilNext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrefetchableIterators}. */
@RunWith(JUnit4.class)
public class PrefetchableIterablesTest {
  @Test
  public void testEmptyIterable() {
    verifyIterable(PrefetchableIterables.emptyIterable());
  }

  @Test
  public void testFromArray() {
    verifyIterable(PrefetchableIterables.fromArray("A", "B", "C"), "A", "B", "C");
    verifyIterable(PrefetchableIterables.fromArray());
  }

  @Test
  public void testLimit() {
    verifyIterable(PrefetchableIterables.limit(PrefetchableIterables.fromArray(), 0));
    verifyIterable(PrefetchableIterables.limit(PrefetchableIterables.fromArray(), 1));
    verifyIterable(PrefetchableIterables.limit(PrefetchableIterables.fromArray("A", "B", "C"), 0));
    verifyIterable(
        PrefetchableIterables.limit(PrefetchableIterables.fromArray("A", "B", "C"), 2), "A", "B");
    verifyIterable(
        PrefetchableIterables.limit(PrefetchableIterables.fromArray("A", "B", "C"), 3),
        "A",
        "B",
        "C");
    verifyIterable(
        PrefetchableIterables.limit(PrefetchableIterables.fromArray("A", "B", "C"), 4),
        "A",
        "B",
        "C");
  }

  @Test
  public void testDefaultPrefetch() {
    PrefetchableIterable<String> iterable =
        new Default<String>() {
          @Override
          protected PrefetchableIterator<String> createIterator() {
            return new ReadyAfterPrefetchUntilNext<>(
                PrefetchableIterators.fromArray("A", "B", "C"));
          }
        };

    assertFalse(iterable.iterator().isReady());
    iterable.prefetch();
    assertTrue(iterable.iterator().isReady());
  }

  @Test
  public void testConcat() {
    verifyIterable(PrefetchableIterables.concat());

    PrefetchableIterable<String> instance = PrefetchableIterables.fromArray("A", "B");
    assertSame(PrefetchableIterables.concat(instance), instance);

    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray()));
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray("A", "B"),
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray()),
        "A",
        "B");
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray("C", "D"),
            PrefetchableIterables.fromArray()),
        "C",
        "D");
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray("E", "F")),
        "E",
        "F");
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray("C", "D"),
            PrefetchableIterables.fromArray("E", "F")),
        "C",
        "D",
        "E",
        "F");
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray("A", "B"),
            PrefetchableIterables.fromArray(),
            PrefetchableIterables.fromArray("E", "F")),
        "A",
        "B",
        "E",
        "F");
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray("A", "B"),
            PrefetchableIterables.fromArray("C", "D"),
            PrefetchableIterables.fromArray()),
        "A",
        "B",
        "C",
        "D");
    verifyIterable(
        PrefetchableIterables.concat(
            PrefetchableIterables.fromArray("A", "B"),
            PrefetchableIterables.fromArray("C", "D"),
            PrefetchableIterables.fromArray("E", "F")),
        "A",
        "B",
        "C",
        "D",
        "E",
        "F");
  }

  public static <T> void verifyIterable(Iterable<T> iterable, T... expected) {
    // Ensure we can access the iterator multiple times
    for (int i = 0; i < 3; i++) {
      PrefetchableIteratorsTest.verifyIterator(iterable.iterator(), expected);
    }
  }
}
