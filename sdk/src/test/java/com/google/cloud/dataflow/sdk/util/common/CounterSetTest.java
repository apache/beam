/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SET;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link CounterSet}.
 */
@RunWith(JUnit4.class)
public class CounterSetTest {
  @Test
  public void testSet() {
    CounterSet set = new CounterSet();
    assertTrue(set.add(Counter.longs("c1", SUM)));
    assertFalse(set.add(Counter.longs("c1", SUM)));
    assertTrue(set.add(Counter.longs("c2", MAX)));
    assertEquals(2, set.size());
  }

  @Test
  public void testAddCounterMutator() {
    CounterSet set = new CounterSet();
    Counter c1 = Counter.longs("c1", SUM);
    Counter c1SecondInstance = Counter.longs("c1", SUM);
    Counter c1IncompatibleInstance = Counter.longs("c1", SET);
    Counter c2 = Counter.longs("c2", MAX);
    Counter c2IncompatibleInstance = Counter.doubles("c2", MAX);

    assertEquals(c1, set.getAddCounterMutator().addCounter(c1));
    assertEquals(c2, set.getAddCounterMutator().addCounter(c2));

    assertEquals(c1, set.getAddCounterMutator().addCounter(c1SecondInstance));

    try {
      set.getAddCounterMutator().addCounter(c1IncompatibleInstance);
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      // Expected.
    }

    try {
      set.getAddCounterMutator().addCounter(c2IncompatibleInstance);
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      // Expected.
    }

    assertEquals(2, set.size());
  }
}
