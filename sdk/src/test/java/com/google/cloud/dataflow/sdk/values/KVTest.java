/*
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
 */

package com.google.cloud.dataflow.sdk.values;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Comparator;

/**
 * Tests for KV.
 */
@RunWith(JUnit4.class)
public class KVTest {
  private static final Integer TEST_VALUES[] =
      {null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE};

  // Wrapper around Integer.compareTo() to support null values.
  private int compareInt(Integer a, Integer b) {
    if (a == null) {
      return b == null ? 0 : -1;
    } else {
      return b == null ? 1 : a.compareTo(b);
    }
  }

  @Test
  public void testOrderByKey() {
    Comparator<KV<Integer, Integer>> orderByKey = new KV.OrderByKey<>();
    for (Integer key1 : TEST_VALUES) {
      for (Integer val1 : TEST_VALUES) {
        for (Integer key2 : TEST_VALUES) {
          for (Integer val2 : TEST_VALUES) {
            assertEquals(compareInt(key1, key2),
                orderByKey.compare(KV.of(key1, val1), KV.of(key2, val2)));
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
            assertEquals(compareInt(val1, val2),
                orderByValue.compare(KV.of(key1, val1), KV.of(key2, val2)));
          }
        }
      }
    }
  }
}
