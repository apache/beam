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
package org.apache.beam.fn.harness.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LazyCachingIteratorToIterable}. */
@RunWith(JUnit4.class)
public class LazyCachingIteratorToIterableTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEmptyIterator() {
    Iterable<Object> iterable = new LazyCachingIteratorToIterable<>(Iterators.forArray());
    assertArrayEquals(new Object[0], Iterables.toArray(iterable, Object.class));
    // iterate multiple times
    assertArrayEquals(new Object[0], Iterables.toArray(iterable, Object.class));

    thrown.expect(NoSuchElementException.class);
    iterable.iterator().next();
  }

  @Test
  public void testInterleavedIteration() {
    Iterable<String> iterable =
        new LazyCachingIteratorToIterable<>(Iterators.forArray("A", "B", "C"));

    Iterator<String> iterator1 = iterable.iterator();
    assertTrue(iterator1.hasNext());
    assertEquals("A", iterator1.next());
    Iterator<String> iterator2 = iterable.iterator();
    assertTrue(iterator2.hasNext());
    assertEquals("A", iterator2.next());
    assertTrue(iterator2.hasNext());
    assertEquals("B", iterator2.next());
    assertTrue(iterator1.hasNext());
    assertEquals("B", iterator1.next());
    assertTrue(iterator1.hasNext());
    assertEquals("C", iterator1.next());
    assertFalse(iterator1.hasNext());
    assertTrue(iterator2.hasNext());
    assertEquals("C", iterator2.next());
    assertFalse(iterator2.hasNext());

    thrown.expect(NoSuchElementException.class);
    iterator1.next();
  }

  @Test
  public void testEqualsAndHashCode() {
    Iterable<String> iterA = new LazyCachingIteratorToIterable<>(Iterators.forArray("A", "B", "C"));
    Iterable<String> iterB = new LazyCachingIteratorToIterable<>(Iterators.forArray("A", "B", "C"));
    Iterable<String> iterC = new LazyCachingIteratorToIterable<>(Iterators.forArray());
    Iterable<String> iterD = new LazyCachingIteratorToIterable<>(Iterators.forArray());
    assertEquals(iterA, iterB);
    assertEquals(iterC, iterD);
    assertNotEquals(iterA, iterC);
    assertEquals(iterA.hashCode(), iterB.hashCode());
    assertEquals(iterC.hashCode(), iterD.hashCode());
  }
}
