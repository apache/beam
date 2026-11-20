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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Observer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ElementByteSizeObservableIterator} and {@link ElementByteSizeObserver}. */
@RunWith(JUnit4.class)
public class ElementByteSizeObservableIteratorTest {

  private static final class TestObservableIterator
      extends ElementByteSizeObservableIterator<Integer> {
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

  private static final class RecordingByteSizeObserver extends ElementByteSizeObserver {
    final List<Long> reported = new ArrayList<>();

    @Override
    protected void reportElementSize(long elementByteSize) {
      reported.add(elementByteSize);
    }
  }

  @Test
  public void testNoObserversAndNullRejection() {
    TestObservableIterator iter = new TestObservableIterator();
    assertThat(iter.countObservers(), equalTo(0));
    iter.triggerNotify(42L);
    assertFalse(iter.hasChanged());

    assertThrows(NullPointerException.class, () -> iter.addObserver(null));
    iter.deleteObserver(null);
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

    iter.triggerNotify(12L);
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
  public void testSingleGenericObserverAndDeleteBehavior() {
    TestObservableIterator iter = new TestObservableIterator();
    List<Object> received = new ArrayList<>();
    Observer generic = (obs, arg) -> received.add(arg);

    iter.addObserver(generic);
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(99L);
    assertThat(received, contains(99L));

    iter.deleteObserver(generic);
    assertThat(iter.countObservers(), equalTo(0));
    iter.triggerNotify(100L);
    assertThat(received, contains(99L));
  }

  @Test
  public void testMultipleObserversReverseOrderAndPromotionOnDelete() {
    TestObservableIterator iter = new TestObservableIterator();
    List<String> order = new ArrayList<>();
    Observer o1 = (obs, arg) -> order.add("o1:" + arg);
    Observer o2 = (obs, arg) -> order.add("o2:" + arg);
    Observer o3 = (obs, arg) -> order.add("o3:" + arg);

    iter.addObserver(o1);
    iter.addObserver(o2);
    iter.addObserver(o3);
    iter.addObserver(o2); // duplicate ignored
    assertThat(iter.countObservers(), equalTo(3));

    iter.triggerNotify(7L);
    // java.util.Observable notifies observers in reverse registration order.
    assertThat(order, contains("o3:7", "o2:7", "o1:7"));

    // Deleting first observer promotes o2 into singleObserver slot.
    order.clear();
    iter.deleteObserver(o1);
    assertThat(iter.countObservers(), equalTo(2));
    iter.triggerNotify(8L);
    assertThat(order, contains("o3:8", "o2:8"));

    // Deleting o3 leaves single observer o2.
    order.clear();
    iter.deleteObserver(o3);
    assertThat(iter.countObservers(), equalTo(1));
    iter.triggerNotify(9L);
    assertThat(order, contains("o2:9"));

    iter.deleteObservers();
    assertThat(iter.countObservers(), equalTo(0));
  }

  @Test
  public void testChangedFlagAndNotifyObservers() {
    TestObservableIterator iter = new TestObservableIterator();
    List<Object> received = new ArrayList<>();
    iter.addObserver((obs, arg) -> received.add(arg));

    iter.notifyObservers("ignored");
    assertTrue(received.isEmpty());

    iter.markChanged();
    assertTrue(iter.hasChanged());
    iter.unmarkChanged();
    assertFalse(iter.hasChanged());

    iter.markChanged();
    iter.notifyObservers();
    assertFalse(iter.hasChanged());
    assertThat(received, contains((Object) null));
  }
}
