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
package org.apache.beam.sdk.util.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ElementByteSizeObservableIterator}, {@link ElementByteSizeObservableIterable},
 * and {@link ElementByteSizeObserver}.
 */
@RunWith(JUnit4.class)
public class ElementByteSizeObservableIteratorTest {

  private static class TestObservableIterator extends ElementByteSizeObservableIterator<Integer> {
    private int nextVal = 0;

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public Integer next() {
      notifyValueReturned(10L);
      return nextVal++;
    }

    void triggerNotify(long size) {
      notifyValueReturned(size);
    }

    void markChanged() {
      setChanged();
    }

    void unmarkChanged() {
      clearChanged();
    }
  }

  private static final class TestObservableIterable
      extends ElementByteSizeObservableIterable<Integer, TestObservableIterator> {
    @Override
    protected TestObservableIterator createIterator() {
      return new TestObservableIterator();
    }
  }

  private static final class RecordingByteSizeObserver extends ElementByteSizeObserver {
    final List<Long> reported = new ArrayList<>();

    @Override
    protected void reportElementSize(long elementByteSize) {
      reported.add(elementByteSize);
    }
  }

  private static final class EqualityObserver implements Observer {
    private final int id;
    final List<Object> seen = new ArrayList<>();

    EqualityObserver(int id) {
      this.id = id;
    }

    @Override
    public void update(Observable o, Object arg) {
      seen.add(arg);
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof EqualityObserver && ((EqualityObserver) obj).id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }

  @Test
  public void testNoObserversClearsChangedFlag() {
    TestObservableIterator iter = new TestObservableIterator();
    assertThat(iter.countObservers(), equalTo(0));
    iter.markChanged();
    assertTrue(iter.hasChanged());
    iter.triggerNotify(42L);
    assertFalse(iter.hasChanged());

    iter.markChanged();
    iter.notifyObservers("ignored");
    assertFalse(iter.hasChanged());
  }

  @Test
  public void testNullRejectionAndNoOpDelete() {
    TestObservableIterator iter = new TestObservableIterator();
    assertThrows(NullPointerException.class, () -> iter.addObserver(null));
    iter.deleteObserver(null);
    iter.deleteObserver((obs, arg) -> {});
    iter.deleteObservers();
    assertThat(iter.countObservers(), equalTo(0));
  }

  @Test
  public void testSingleElementByteSizeObserverFastPath() {
    TestObservableIterator iter = new TestObservableIterator();
    RecordingByteSizeObserver observer = new RecordingByteSizeObserver();
    iter.addObserver(observer);
    // Duplicate add is ignored per Observable contract.
    iter.addObserver(observer);
    assertThat(iter.countObservers(), equalTo(1));

    iter.markChanged();
    iter.triggerNotify(12L);
    assertFalse(iter.hasChanged());
    iter.triggerNotify(28L);
    observer.advance();
    assertThat(observer.reported, contains(40L));

    // Verify scalingFactor != 1.0 path in primitive update(long).
    observer.setScalingFactor(2.5);
    iter.triggerNotify(10L);
    observer.advance();
    assertThat(observer.reported, contains(40L, 25L));
  }

  @Test
  public void testPrimitiveByteSizeObserverDispatchConsistency() {
    List<Long> primitiveReceived = new ArrayList<>();
    PrimitiveByteSizeObserver customPrimitive =
        new PrimitiveByteSizeObserver() {
          @Override
          public void update(long byteSize) {
            primitiveReceived.add(byteSize);
          }

          @Override
          public void update(Observable o, Object arg) {
            throw new AssertionError("Should use primitive update(long) in notifyValueReturned");
          }
        };

    TestObservableIterator iter = new TestObservableIterator();
    iter.addObserver(customPrimitive);
    iter.triggerNotify(1024L);
    assertThat(primitiveReceived, contains(1024L));

    // Adding a second observer must keep dispatching through update(long) consistently.
    RecordingByteSizeObserver second = new RecordingByteSizeObserver();
    List<Object> genericReceived = new ArrayList<>();
    AtomicReference<Observable> genericSource = new AtomicReference<>();
    Observer generic =
        (obs, arg) -> {
          genericSource.set(obs);
          genericReceived.add(arg);
        };
    iter.addObserver(second);
    iter.addObserver(generic);

    iter.markChanged();
    iter.triggerNotify(2048L);
    assertFalse(iter.hasChanged());
    second.advance();
    assertThat(primitiveReceived, contains(1024L, 2048L));
    assertThat(second.reported, contains(2048L));
    assertThat(genericReceived, contains(2048L));
    assertThat(genericSource.get(), sameInstance(iter));
  }

  @Test
  public void testSingleGenericObserverReceivesObservableReferenceAndDelete() {
    TestObservableIterator iter = new TestObservableIterator();
    List<Object> received = new ArrayList<>();
    AtomicReference<Observable> observedSource = new AtomicReference<>();
    Observer generic =
        (obs, arg) -> {
          observedSource.set(obs);
          received.add(arg);
        };

    iter.addObserver(generic);
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(99L);
    assertThat(observedSource.get(), sameInstance(iter));
    assertThat(received, contains(99L));

    // Deleting unregistered observer is a no-op.
    iter.deleteObserver((obs, arg) -> {});
    assertThat(iter.countObservers(), equalTo(1));

    iter.deleteObserver(generic);
    assertThat(iter.countObservers(), equalTo(0));
    iter.triggerNotify(100L);
    assertThat(received, contains(99L));
  }

  @Test
  public void testMultipleObserversReverseOrderAndDeleteEdgeCases() {
    TestObservableIterator iter = new TestObservableIterator();
    List<String> order = new ArrayList<>();
    Observer o1 = (obs, arg) -> order.add("o1:" + arg);
    Observer o2 = (obs, arg) -> order.add("o2:" + arg);
    Observer o3 = (obs, arg) -> order.add("o3:" + arg);

    iter.addObserver(o1);
    iter.addObserver(o2);
    iter.addObserver(o3);
    iter.addObserver(o1); // duplicate of first element ignored
    iter.addObserver(o2); // duplicate of middle element ignored
    iter.addObserver(o3); // duplicate of last element ignored
    assertThat(iter.countObservers(), equalTo(3));

    // deleteObserver(null) must be a no-op with 2+ observers registered.
    iter.deleteObserver(null);
    assertThat(iter.countObservers(), equalTo(3));

    // java.util.Observable notifies observers in reverse registration order.
    iter.triggerNotify(7L);
    assertThat(order, contains("o3:7", "o2:7", "o1:7"));

    // Deleting middle observer preserves order of remaining observers.
    order.clear();
    iter.deleteObserver(o2);
    assertThat(iter.countObservers(), equalTo(2));
    iter.deleteObserver((obs, arg) -> {}); // unregistered no-op
    assertThat(iter.countObservers(), equalTo(2));
    iter.markChanged();
    iter.notifyObservers("msg");
    assertThat(order, contains("o3:msg", "o1:msg"));

    // Deleting first observer (i == 0 of 2) collapses back to single observer slot (o3).
    order.clear();
    iter.deleteObserver(o1);
    assertThat(iter.countObservers(), equalTo(1));
    iter.deleteObserver(null); // no-op with 1 observer registered
    assertThat(iter.countObservers(), equalTo(1));
    iter.addObserver(o3); // duplicate after collapse ignored
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(9L);
    assertThat(order, contains("o3:9"));

    // Deleting second observer (i == 1 of 2) collapses back to single observer slot (o3).
    order.clear();
    iter.addObserver(o2);
    assertThat(iter.countObservers(), equalTo(2));
    iter.deleteObserver(o2);
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(10L);
    assertThat(order, contains("o3:10"));

    // Deleting first (i == 0) and last (i == len - 1) from a 3-observer array.
    order.clear();
    iter.deleteObservers();
    iter.addObserver(o1);
    iter.addObserver(o2);
    iter.addObserver(o3);
    iter.deleteObserver(o1); // i == 0 of 3
    iter.triggerNotify(12L);
    assertThat(order, contains("o3:12", "o2:12"));

    order.clear();
    iter.addObserver(o1); // [o2, o3, o1]
    iter.deleteObserver(o1); // i == 2 (last) of 3
    iter.triggerNotify(13L);
    assertThat(order, contains("o3:13", "o2:13"));

    // Lazy single-Long box is shared across multiple plain Observers for uncached values (> 127).
    List<Object> boxed1 = new ArrayList<>();
    List<Object> boxed2 = new ArrayList<>();
    TestObservableIterator sharedBoxIter = new TestObservableIterator();
    sharedBoxIter.addObserver((obs, arg) -> boxed1.add(arg));
    sharedBoxIter.addObserver((obs, arg) -> boxed2.add(arg));
    sharedBoxIter.triggerNotify(10_000L);
    assertThat(boxed1, contains(10_000L));
    assertThat(boxed2.get(0), sameInstance(boxed1.get(0)));

    // Re-adding after deleteObservers().
    iter.deleteObservers();
    assertThat(iter.countObservers(), equalTo(0));
    order.clear();
    iter.addObserver(o1);
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(11L);
    assertThat(order, contains("o1:11"));
  }

  @Test
  public void testEqualsBasedDeduplicationAndDelete() {
    TestObservableIterator iter = new TestObservableIterator();
    EqualityObserver o1a = new EqualityObserver(1);
    EqualityObserver o1b = new EqualityObserver(1);
    EqualityObserver o2a = new EqualityObserver(2);
    EqualityObserver o2b = new EqualityObserver(2);

    iter.addObserver(o1a);
    iter.addObserver(o1b); // equal to single observer -> ignored
    assertThat(iter.countObservers(), equalTo(1));

    iter.addObserver(o2a);
    iter.addObserver(o2b); // equal to entry in array -> ignored
    assertThat(iter.countObservers(), equalTo(2));

    iter.deleteObserver(o1b); // deletes o1a via equals()
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(5L);
    assertTrue(o1a.seen.isEmpty());
    assertThat(o2a.seen, contains(5L));

    iter.deleteObserver(o2b); // deletes o2a via equals()
    assertThat(iter.countObservers(), equalTo(0));
  }

  @Test
  public void testReentrantMutationUsesImmutableSnapshot() {
    TestObservableIterator iter = new TestObservableIterator();
    List<String> log = new ArrayList<>();
    Observer addedDuringCallback = (obs, arg) -> log.add("late:" + arg);
    AtomicBoolean changedInsideCallback = new AtomicBoolean(true);

    Observer[] holder = new Observer[2];
    holder[0] = (obs, arg) -> log.add("first:" + arg);
    holder[1] =
        (obs, arg) -> {
          changedInsideCallback.set(iter.hasChanged());
          log.add("second:" + arg);
          // Mutate observer set during notification: neither removal nor addition affects this
          // wave.
          iter.deleteObserver(holder[0]);
          iter.addObserver(addedDuringCallback);
        };
    iter.addObserver(holder[0]);
    iter.addObserver(holder[1]);

    iter.triggerNotify(50L);
    assertFalse(changedInsideCallback.get());
    assertThat(log, contains("second:50", "first:50"));

    // Subsequent wave sees the mutated observers: holder[1] and addedDuringCallback.
    log.clear();
    iter.markChanged();
    iter.notifyObservers("next");
    assertThat(log, contains("late:next", "second:next"));

    // Re-entrant deleteObservers() from inside a single-observer callback.
    TestObservableIterator single = new TestObservableIterator();
    single.addObserver(
        (obs, arg) -> {
          assertFalse(single.hasChanged());
          single.deleteObservers();
        });
    single.triggerNotify(1L);
    assertThat(single.countObservers(), equalTo(0));
  }

  @Test
  public void testExceptionInObserverStillLeavesChangedCleared() {
    TestObservableIterator iter = new TestObservableIterator();
    List<String> called = new ArrayList<>();
    iter.addObserver((obs, arg) -> called.add("first"));
    iter.addObserver(
        (obs, arg) -> {
          throw new IllegalStateException("boom");
        });

    iter.markChanged();
    assertThrows(IllegalStateException.class, () -> iter.notifyObservers("payload"));
    assertFalse(iter.hasChanged());
    assertTrue(called.isEmpty());

    iter.markChanged();
    assertThrows(IllegalStateException.class, () -> iter.triggerNotify(100L));
    assertFalse(iter.hasChanged());
  }

  @Test
  public void testChangedFlagAndClearChangedHook() {
    List<String> hookCalls = new ArrayList<>();
    TestObservableIterator iter =
        new TestObservableIterator() {
          @Override
          @SuppressWarnings("UnsynchronizedOverridesSynchronized")
          protected void setChanged() {
            hookCalls.add("setChanged");
            super.setChanged();
          }

          @Override
          @SuppressWarnings("UnsynchronizedOverridesSynchronized")
          protected void clearChanged() {
            hookCalls.add("clearChanged");
            super.clearChanged();
          }

          @Override
          public void notifyObservers(Object arg) {
            hookCalls.add("notifyObservers");
            super.notifyObservers(arg);
          }
        };
    List<Object> received = new ArrayList<>();
    iter.addObserver((obs, arg) -> received.add(arg));

    iter.notifyObservers("ignored");
    assertTrue(received.isEmpty());
    assertThat(hookCalls, contains("notifyObservers"));
    hookCalls.clear();

    iter.markChanged();
    assertTrue(iter.hasChanged());
    iter.unmarkChanged();
    assertFalse(iter.hasChanged());
    assertThat(hookCalls, contains("setChanged", "clearChanged"));
    hookCalls.clear();

    iter.markChanged();
    iter.notifyObservers();
    assertFalse(iter.hasChanged());
    assertThat(received, contains(nullValue()));
    assertThat(hookCalls, contains("setChanged", "notifyObservers", "clearChanged"));
    hookCalls.clear();

    // notifyValueReturned bypasses setChanged()/notifyObservers() and skips clearChanged() when
    // changed is already false.
    iter.triggerNotify(42L);
    assertTrue(hookCalls.isEmpty());

    // When changed is true prior to notifyValueReturned, clearChanged() IS invoked (still skipping
    // notifyObservers()).
    iter.markChanged();
    hookCalls.clear();
    iter.triggerNotify(43L);
    assertFalse(iter.hasChanged());
    assertThat(hookCalls, contains("clearChanged"));
  }

  @Test
  public void testElementByteSizeObserverOverloadsPrecisionAndState() {
    RecordingByteSizeObserver observer = new RecordingByteSizeObserver();
    assertFalse(observer.getIsLazy());
    observer.setLazy();
    assertTrue(observer.getIsLazy());

    // Uncast boxed Long / Integer, Object-typed Long / Integer, primitive int / long.
    observer.update(Long.valueOf(10L));
    observer.update(Integer.valueOf(20));
    observer.update((Object) Long.valueOf(1L));
    observer.update((Object) Integer.valueOf(2));
    observer.update(30);
    observer.update(40L);
    observer.update(null, Long.valueOf(5L));
    observer.update(null, Integer.valueOf(6));
    observer.advance();
    assertFalse(observer.getIsLazy());
    assertThat(observer.reported, contains(114L));
    // Repeated advance() without updates reports 0L (totalSize reset).
    observer.advance();
    assertThat(observer.reported.get(1), equalTo(0L));

    // Exact long precision above 2^53 when scalingFactor == 1.0 (no double rounding).
    long large = (1L << 53) + 1L;
    observer.update(large);
    observer.advance();
    assertThat(observer.reported.get(2), equalTo(large));

    // Zero, fractional, negative, and NaN scaling factors (including boxed/legacy update paths).
    observer.setScalingFactor(0.0);
    observer.update(999L);
    observer.advance();
    assertThat(observer.reported.get(3), equalTo(0L));

    observer.setScalingFactor(1.5);
    observer.update(3L); // (long) (1.5 * 3) == 4
    observer.update(Long.valueOf(3L)); // 4
    observer.update(null, Integer.valueOf(3)); // 4
    observer.advance();
    assertThat(observer.reported.get(4), equalTo(12L));

    observer.setScalingFactor(-2.0);
    observer.update(10L);
    observer.advance();
    assertThat(observer.reported.get(5), equalTo(-20L));

    observer.setScalingFactor(Double.NaN);
    observer.update(10L); // (long) NaN == 0
    observer.advance();
    assertThat(observer.reported.get(6), equalTo(0L));

    observer.setScalingFactor(1.0);
    observer.update(0L);
    observer.update(-5L);
    observer.advance();
    assertThat(observer.reported.get(7), equalTo(-5L));

    // Unsupported types and null boxed Long/Integer reject with AssertionError (no NPE from
    // unboxing per JLS §15.12.2 Phase 1 strict invocation).
    assertThrows(AssertionError.class, () -> observer.update("bad"));
    assertThrows(AssertionError.class, () -> observer.update((Object) null));
    assertThrows(AssertionError.class, () -> observer.update((Long) null));
    assertThrows(AssertionError.class, () -> observer.update((Integer) null));
    assertThrows(AssertionError.class, () -> observer.update(null, "bad"));
    assertThrows(AssertionError.class, () -> observer.update(Byte.valueOf((byte) 1)));
  }

  @Test
  public void testObservableIterableLazyTransferLifecycle() {
    TestObservableIterable iterable = new TestObservableIterable();
    // No observers attached.
    assertThat(iterable.iterator().countObservers(), equalTo(0));

    // Null rejection.
    assertThrows(NullPointerException.class, () -> iterable.addObserver(null));

    // Single observer transferred once and cleared; addObserver(null) preserves existing observer;
    // duplicate observer is deduplicated.
    RecordingByteSizeObserver o1 = new RecordingByteSizeObserver();
    iterable.addObserver(o1);
    iterable.addObserver(o1);
    assertThrows(NullPointerException.class, () -> iterable.addObserver(null));
    TestObservableIterator it1 = iterable.iterator();
    assertThat(it1.countObservers(), equalTo(1));
    it1.next();
    o1.advance();
    assertThat(o1.reported, contains(10L));

    // Subsequent iterator is unobserved.
    assertThat(iterable.iterator().countObservers(), equalTo(0));

    // Multiple observers (2 -> 3, with duplicate ignored) transferred in registration order and
    // cleared.
    List<String> order = new ArrayList<>();
    Observer g1 = (obs, arg) -> order.add("g1:" + arg);
    Observer g2 = (obs, arg) -> order.add("g2:" + arg);
    Observer g3 = (obs, arg) -> order.add("g3:" + arg);
    iterable.addObserver(g1);
    iterable.addObserver(g2);
    iterable.addObserver(g1); // duplicate ignored
    iterable.addObserver(g3);
    iterable.addObserver(g3); // duplicate ignored
    TestObservableIterator it2 = iterable.iterator();
    assertThat(it2.countObservers(), equalTo(3));
    it2.triggerNotify(7L);
    assertThat(order, contains("g3:7", "g2:7", "g1:7"));
    assertThat(iterable.iterator().countObservers(), equalTo(0));

    // Re-entrant addObserver() inside iterator.addObserver() is preserved for the NEXT iterator().
    RecordingByteSizeObserver reentrantNext = new RecordingByteSizeObserver();
    ElementByteSizeObservableIterable<Integer, TestObservableIterator> reentrantIterable =
        new ElementByteSizeObservableIterable<Integer, TestObservableIterator>() {
          private boolean first = true;

          @Override
          protected TestObservableIterator createIterator() {
            if (first) {
              first = false;
              ElementByteSizeObservableIterable<Integer, TestObservableIterator> outer = this;
              return new TestObservableIterator() {
                @Override
                @SuppressWarnings("UnsynchronizedOverridesSynchronized")
                public void addObserver(Observer o) {
                  super.addObserver(o);
                  outer.addObserver(reentrantNext);
                }
              };
            }
            return new TestObservableIterator();
          }
        };
    RecordingByteSizeObserver initial = new RecordingByteSizeObserver();
    reentrantIterable.addObserver(initial);
    TestObservableIterator firstIt = reentrantIterable.iterator();
    assertThat(firstIt.countObservers(), equalTo(1));
    TestObservableIterator secondIt = reentrantIterable.iterator();
    assertThat(secondIt.countObservers(), equalTo(1));
    secondIt.next();
    reentrantNext.advance();
    assertThat(reentrantNext.reported, contains(10L));
    assertThat(reentrantIterable.iterator().countObservers(), equalTo(0));

    // Re-adding an observer after clearing works normally.
    RecordingByteSizeObserver o2 = new RecordingByteSizeObserver();
    iterable.addObserver(o2);
    assertThat(iterable.iterator().countObservers(), equalTo(1));
  }
}
