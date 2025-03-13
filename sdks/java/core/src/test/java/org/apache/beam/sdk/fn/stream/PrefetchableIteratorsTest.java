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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrefetchableIterators}. */
@RunWith(JUnit4.class)
public class PrefetchableIteratorsTest {

  @Test
  public void testEmpty() {
    verifyIterator(PrefetchableIterators.emptyIterator());
    verifyIsAlwaysReady(PrefetchableIterators.emptyIterator());
  }

  @Test
  public void testFromArray() {
    verifyIterator(PrefetchableIterators.fromArray("A", "B", "C"), "A", "B", "C");
    verifyIsAlwaysReady(PrefetchableIterators.fromArray("A", "B", "C"));
    verifyIterator(PrefetchableIterators.fromArray());
    verifyIsAlwaysReady(PrefetchableIterators.fromArray());
  }

  @Test
  public void testConcat() {
    verifyIterator(PrefetchableIterators.concat());

    PrefetchableIterator<String> instance = PrefetchableIterators.fromArray("A", "B");
    assertSame(PrefetchableIterators.concat(instance), instance);

    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray()));
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray("A", "B"),
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray()),
        "A",
        "B");
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray("C", "D"),
            PrefetchableIterators.fromArray()),
        "C",
        "D");
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray("E", "F")),
        "E",
        "F");
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray("C", "D"),
            PrefetchableIterators.fromArray("E", "F")),
        "C",
        "D",
        "E",
        "F");
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray("A", "B"),
            PrefetchableIterators.fromArray(),
            PrefetchableIterators.fromArray("E", "F")),
        "A",
        "B",
        "E",
        "F");
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray("A", "B"),
            PrefetchableIterators.fromArray("C", "D"),
            PrefetchableIterators.fromArray()),
        "A",
        "B",
        "C",
        "D");
    verifyIterator(
        PrefetchableIterators.concat(
            PrefetchableIterators.fromArray("A", "B"),
            PrefetchableIterators.fromArray("C", "D"),
            PrefetchableIterators.fromArray("E", "F")),
        "A",
        "B",
        "C",
        "D",
        "E",
        "F");
  }

  public static class NeverReady<T> implements PrefetchableIterator<T> {
    private final Iterator<T> delegate;
    int prefetchCalled;

    public NeverReady(Iterator<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void prefetch() {
      prefetchCalled += 1;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public T next() {
      return delegate.next();
    }

    public int getNumPrefetchCalls() {
      return prefetchCalled;
    }
  }

  public static class ReadyAfterPrefetch<T> extends NeverReady<T> {

    public ReadyAfterPrefetch(Iterator<T> delegate) {
      super(delegate);
    }

    @Override
    public boolean isReady() {
      return prefetchCalled > 0;
    }
  }

  public static class ReadyAfterPrefetchUntilNext<T> extends ReadyAfterPrefetch<T> {
    boolean advancedSincePrefetch;

    public ReadyAfterPrefetchUntilNext(Iterator<T> delegate) {
      super(delegate);
    }

    @Override
    public boolean isReady() {
      return !advancedSincePrefetch && super.isReady();
    }

    @Override
    public void prefetch() {
      advancedSincePrefetch = false;
      super.prefetch();
    }

    @Override
    public T next() {
      advancedSincePrefetch = true;
      return super.next();
    }

    @Override
    public boolean hasNext() {
      advancedSincePrefetch = true;
      return super.hasNext();
    }
  }

  @Test
  public void testConcatIsReadyAdvancesToNextIteratorWhenAble() {
    NeverReady<String> readyAfterPrefetch1 =
        new NeverReady<>(PrefetchableIterators.fromArray("A", "B"));
    ReadyAfterPrefetch<String> readyAfterPrefetch2 =
        new ReadyAfterPrefetch<>(PrefetchableIterators.fromArray("A", "B"));
    ReadyAfterPrefetch<String> readyAfterPrefetch3 =
        new ReadyAfterPrefetch<>(PrefetchableIterators.fromArray("A", "B"));

    PrefetchableIterator<String> iterator =
        PrefetchableIterators.concat(readyAfterPrefetch1, readyAfterPrefetch2, readyAfterPrefetch3);

    // Expect no prefetches yet
    assertEquals(0, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch3.getNumPrefetchCalls());

    // We expect to attempt to prefetch for the first time.
    iterator.prefetch();
    assertEquals(1, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch3.getNumPrefetchCalls());
    iterator.next();

    // We expect to attempt to prefetch again since we aren't ready.
    iterator.prefetch();
    assertEquals(2, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch3.getNumPrefetchCalls());
    iterator.next();

    // The current iterator is done but is never ready so we can't advance to the next one and
    // expect another prefetch to go to the current iterator.
    iterator.prefetch();
    assertEquals(3, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch3.getNumPrefetchCalls());
    iterator.next();

    // Now that we know the last iterator is done and have advanced to the next one we expect
    // prefetch to go through
    iterator.prefetch();
    assertEquals(3, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(1, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(0, readyAfterPrefetch3.getNumPrefetchCalls());
    iterator.next();

    // The last iterator is done so we should be able to prefetch the next one before advancing
    iterator.prefetch();
    assertEquals(3, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(1, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(1, readyAfterPrefetch3.getNumPrefetchCalls());
    iterator.next();

    // The current iterator is ready so no additional prefetch is necessary
    iterator.prefetch();
    assertEquals(3, readyAfterPrefetch1.getNumPrefetchCalls());
    assertEquals(1, readyAfterPrefetch2.getNumPrefetchCalls());
    assertEquals(1, readyAfterPrefetch3.getNumPrefetchCalls());
    iterator.next();
  }

  public static <T> void verifyIsAlwaysReady(PrefetchableIterator<T> iterator) {
    while (iterator.hasNext()) {
      assertTrue(iterator.isReady());
      iterator.next();
    }
    assertTrue(iterator.isReady());
  }

  public static <T> void verifyIterator(Iterator<T> iterator, T... expected) {
    for (int i = 0; i < expected.length; ++i) {
      assertTrue(iterator.hasNext());
      assertEquals(expected[i], iterator.next());
    }
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> iterator.next());
    // Ensure that multiple hasNext/next after a failure are repeatable
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> iterator.next());
  }
}
