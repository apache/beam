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
package org.apache.beam.sdk.values;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Comparator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for KV. */
@RunWith(JUnit4.class)
public class KVTest {
  private static final Integer[] TEST_VALUES = {
    null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE
  };

  // Wrapper around Integer.compareTo() to support null values.
  private int compareInt(Integer a, Integer b) {
    if (a == null) {
      return b == null ? 0 : -1;
    } else {
      return b == null ? 1 : a.compareTo(b);
    }
  }

  @Test
  public void testEquals() {
    // Neither position are arrays
    assertThat(KV.of(1, 2), equalTo(KV.of(1, 2)));

    // Key is array
    assertThat(KV.of(new int[] {1, 2}, 3), equalTo(KV.of(new int[] {1, 2}, 3)));

    // Value is array
    assertThat(KV.of(1, new int[] {2, 3}), equalTo(KV.of(1, new int[] {2, 3})));

    // Both are arrays
    assertThat(
        KV.of(new int[] {1, 2}, new int[] {3, 4}),
        equalTo(KV.of(new int[] {1, 2}, new int[] {3, 4})));

    // Unfortunately, deep equals only goes so far
    assertThat(
        KV.of(ImmutableList.of(new int[] {1, 2}), 3),
        not(equalTo(KV.of(ImmutableList.of(new int[] {1, 2}), 3))));
    assertThat(
        KV.of(1, ImmutableList.of(new int[] {2, 3})),
        not(equalTo(KV.of(1, ImmutableList.of(new int[] {2, 3})))));

    // Key is array and differs
    assertThat(KV.of(new int[] {1, 2}, 3), not(equalTo(KV.of(new int[] {1, 37}, 3))));

    // Key is non-array and differs
    assertThat(KV.of(1, new int[] {2, 3}), not(equalTo(KV.of(37, new int[] {1, 2}))));

    // Value is array and differs
    assertThat(KV.of(1, new int[] {2, 3}), not(equalTo(KV.of(1, new int[] {37, 3}))));

    // Value is non-array and differs
    assertThat(KV.of(new byte[] {1, 2}, 3), not(equalTo(KV.of(new byte[] {1, 2}, 37))));
  }

  @Test
  public void testOrderByKey() {
    Comparator<KV<Integer, Integer>> orderByKey = new KV.OrderByKey<>();
    for (Integer key1 : TEST_VALUES) {
      for (Integer val1 : TEST_VALUES) {
        for (Integer key2 : TEST_VALUES) {
          for (Integer val2 : TEST_VALUES) {
            assertEquals(
                compareInt(key1, key2), orderByKey.compare(KV.of(key1, val1), KV.of(key2, val2)));
          }
        }
      }
    }
  }

  @Test
  public void testOrderByValue() {
    Comparator<KV<Integer, Integer>> orderByValue = new KV.OrderByValue<>();
    for (Integer key1 : TEST_VALUES) {
      for (Integer val1 : TEST_VALUES) {
        for (Integer key2 : TEST_VALUES) {
          for (Integer val2 : TEST_VALUES) {
            assertEquals(
                compareInt(val1, val2), orderByValue.compare(KV.of(key1, val1), KV.of(key2, val2)));
          }
        }
      }
    }
  }
}
