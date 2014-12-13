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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Tests of TestShuffleReader.
 */
@RunWith(JUnit4.class)
public class TestShuffleReaderTest {
  static final String START_KEY = "ddd";
  static final String END_KEY = "mmm";

  static final List<Map.Entry<String, String>> NO_ENTRIES =
      Collections.emptyList();

  static final List<Map.Entry<String, String>> IN_RANGE_ENTRIES =
      Arrays.<Map.Entry<String, String>>asList(
          new SimpleEntry<>("ddd", "in 1"),
          new SimpleEntry<>("ddd", "in 1"),
          new SimpleEntry<>("ddd", "in 1"),
          new SimpleEntry<>("dddd", "in 2"),
          new SimpleEntry<>("dddd", "in 2"),
          new SimpleEntry<>("de", "in 3"),
          new SimpleEntry<>("ee", "in 4"),
          new SimpleEntry<>("ee", "in 4"),
          new SimpleEntry<>("ee", "in 4"),
          new SimpleEntry<>("ee", "in 4"),
          new SimpleEntry<>("mm", "in 5"));
  static final List<Map.Entry<String, String>> BEFORE_RANGE_ENTRIES =
      Arrays.<Map.Entry<String, String>>asList(
          new SimpleEntry<>("", "out 1"),
          new SimpleEntry<>("dd", "out 2"));
  static final List<Map.Entry<String, String>> AFTER_RANGE_ENTRIES =
      Arrays.<Map.Entry<String, String>>asList(
          new SimpleEntry<>("mmm", "out 3"),
          new SimpleEntry<>("mmm", "out 3"),
          new SimpleEntry<>("mmmm", "out 4"),
          new SimpleEntry<>("mn", "out 5"),
          new SimpleEntry<>("zzz", "out 6"));
  static final List<Map.Entry<String, String>> OUT_OF_RANGE_ENTRIES =
      new ArrayList<>();
  static {
    OUT_OF_RANGE_ENTRIES.addAll(BEFORE_RANGE_ENTRIES);
    OUT_OF_RANGE_ENTRIES.addAll(AFTER_RANGE_ENTRIES);
  }
  static final List<Map.Entry<String, String>> ALL_ENTRIES = new ArrayList<>();
  static {
    ALL_ENTRIES.addAll(BEFORE_RANGE_ENTRIES);
    ALL_ENTRIES.addAll(IN_RANGE_ENTRIES);
    ALL_ENTRIES.addAll(AFTER_RANGE_ENTRIES);
  }

  void runTest(List<Map.Entry<String, String>> expected,
               List<Map.Entry<String, String>> outOfRange,
               String startKey,
               String endKey) {
    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Map.Entry<String, String>> expectedCopy = new ArrayList<>(expected);
    expectedCopy.addAll(outOfRange);
    Collections.shuffle(expectedCopy);
    for (Map.Entry<String, String> entry : expectedCopy) {
      shuffleReader.addEntry(entry.getKey(), entry.getValue());
    }
    Iterator<ShuffleEntry> iter = shuffleReader.read(startKey, endKey);
    List<Map.Entry<String, String>> actual = new ArrayList<>();
    while (iter.hasNext()) {
      ShuffleEntry entry = iter.next();
      actual.add(new SimpleEntry<>(new String(entry.getKey()),
              new String(entry.getValue())));
    }
    try {
      iter.next();
      Assert.fail("should have failed");
    } catch (NoSuchElementException exn) {
      // Success.
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testEmpty() {
    runTest(NO_ENTRIES, NO_ENTRIES, null, null);
  }

  @Test
  public void testEmptyWithRange() {
    runTest(NO_ENTRIES, NO_ENTRIES, START_KEY, END_KEY);
  }

  @Test
  public void testNonEmpty() {
    runTest(ALL_ENTRIES, NO_ENTRIES, null, null);
  }

  @Test
  public void testNonEmptyWithAllInRange() {
    runTest(IN_RANGE_ENTRIES, NO_ENTRIES, START_KEY, END_KEY);
  }

  @Test
  public void testNonEmptyWithSomeOutOfRange() {
    runTest(IN_RANGE_ENTRIES, OUT_OF_RANGE_ENTRIES, START_KEY, END_KEY);
  }

  @Test
  public void testNonEmptyWithAllOutOfRange() {
    runTest(NO_ENTRIES, OUT_OF_RANGE_ENTRIES, START_KEY, END_KEY);
  }
}
