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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of TestShuffleReader. */
@RunWith(JUnit4.class)
public class TestShuffleReaderTest {
  static final String START_KEY = "ddd";
  static final String END_KEY = "mmm";

  static final List<KV<String, KV<String, String>>> NO_ENTRIES = Collections.emptyList();

  static final List<KV<String, KV<String, String>>> IN_RANGE_ENTRIES =
      Arrays.<KV<String, KV<String, String>>>asList(
          KV.of("ddd", KV.of("1", "in 1")),
          KV.of("ddd", KV.of("2", "in 1")),
          KV.of("ddd", KV.of("3", "in 1")),
          KV.of("dddd", KV.of("1", "in 2")),
          KV.of("dddd", KV.of("2", "in 2")),
          KV.of("de", KV.of("1", "in 3")),
          KV.of("ee", KV.of("1", "in 4")),
          KV.of("ee", KV.of("2", "in 4")),
          KV.of("ee", KV.of("3", "in 4")),
          KV.of("ee", KV.of("4", "in 4")),
          KV.of("mm", KV.of("1", "in 5")));
  static final List<KV<String, KV<String, String>>> BEFORE_RANGE_ENTRIES =
      Arrays.<KV<String, KV<String, String>>>asList(
          KV.of("", KV.of("1", "out 1")), KV.of("dd", KV.of("1", "out 2")));
  static final List<KV<String, KV<String, String>>> AFTER_RANGE_ENTRIES =
      Arrays.<KV<String, KV<String, String>>>asList(
          KV.of("mmm", KV.of("1", "out 3")),
          KV.of("mmm", KV.of("2", "out 3")),
          KV.of("mmmm", KV.of("1", "out 4")),
          KV.of("mn", KV.of("1", "out 5")),
          KV.of("zzz", KV.of("1", "out 6")));
  static final List<KV<String, KV<String, String>>> OUT_OF_RANGE_ENTRIES = new ArrayList<>();

  static {
    OUT_OF_RANGE_ENTRIES.addAll(BEFORE_RANGE_ENTRIES);
    OUT_OF_RANGE_ENTRIES.addAll(AFTER_RANGE_ENTRIES);
  }

  static final List<KV<String, KV<String, String>>> ALL_ENTRIES = new ArrayList<>();

  static {
    ALL_ENTRIES.addAll(BEFORE_RANGE_ENTRIES);
    ALL_ENTRIES.addAll(IN_RANGE_ENTRIES);
    ALL_ENTRIES.addAll(AFTER_RANGE_ENTRIES);
  }

  void runTest(
      List<KV<String, KV<String, String>>> expected,
      List<KV<String, KV<String, String>>> outOfRange,
      String startKey,
      String endKey) {
    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<KV<String, KV<String, String>>> expectedCopy = new ArrayList<>(expected);
    expectedCopy.addAll(outOfRange);
    Collections.shuffle(expectedCopy);
    for (KV<String, KV<String, String>> entry : expectedCopy) {
      shuffleReader.addEntry(
          entry.getKey(),
          entry.getValue().getKey() /* secondary key */,
          entry.getValue().getValue());
    }
    testIteratorReturnsValues(shuffleReader.read(startKey, endKey), expected);
    testIteratorIntermediateCopiesReturnTails(shuffleReader.read(startKey, endKey), expected);
  }

  private void testIteratorReturnsValues(
      Reiterator<ShuffleEntry> iter, List<KV<String, KV<String, String>>> expected) {
    List<KV<String, KV<String, String>>> actual = readShuffleEntries(iter);
    try {
      iter.next();
      fail("should have failed");
    } catch (NoSuchElementException exn) {
      // Success.
    }
    assertEquals(expected, actual);
  }

  private List<KV<String, KV<String, String>>> readShuffleEntries(Reiterator<ShuffleEntry> iter) {
    List<KV<String, KV<String, String>>> actual = new ArrayList<>();
    while (iter.hasNext()) {
      ShuffleEntry entry = iter.next();
      actual.add(
          KV.of(
              new String(entry.getKey(), StandardCharsets.UTF_8),
              KV.of(
                  new String(entry.getSecondaryKey(), StandardCharsets.UTF_8),
                  new String(entry.getValue(), StandardCharsets.UTF_8))));
    }
    return actual;
  }

  private void testIteratorIntermediateCopiesReturnTails(
      Reiterator<ShuffleEntry> iter, List<KV<String, KV<String, String>>> expected) {
    List<Reiterator<ShuffleEntry>> iteratorCopies = new ArrayList<>();
    for (; iter.hasNext(); iter.next()) {
      iteratorCopies.add(iter.copy());
    }
    for (int i = 0; i < expected.size(); ++i) {
      assertEquals(
          "" + i, expected.subList(i, expected.size()), readShuffleEntries(iteratorCopies.get(i)));
    }
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
