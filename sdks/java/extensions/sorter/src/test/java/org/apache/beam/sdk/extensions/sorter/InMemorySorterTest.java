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
package org.apache.beam.sdk.extensions.sorter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InMemorySorter}. */
@RunWith(JUnit4.class)
public class InMemorySorterTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEmpty() throws Exception {
    SorterTestUtils.testEmpty(InMemorySorter.create(new InMemorySorter.Options()));
  }

  @Test
  public void testSingleElement() throws Exception {
    SorterTestUtils.testSingleElement(InMemorySorter.create(new InMemorySorter.Options()));
  }

  @Test
  public void testEmptyKeyValueElement() throws Exception {
    SorterTestUtils.testEmptyKeyValueElement(InMemorySorter.create(new InMemorySorter.Options()));
  }

  @Test
  public void testMultipleIterations() throws Exception {
    SorterTestUtils.testMultipleIterations(InMemorySorter.create(new InMemorySorter.Options()));
  }

  @Test
  public void testManySorters() throws Exception {
    SorterTestUtils.testRandom(
        () -> InMemorySorter.create(new InMemorySorter.Options()), 1000000, 10);
  }

  @Test
  public void testAddAfterSort() throws Exception {
    SorterTestUtils.testAddAfterSort(InMemorySorter.create(new InMemorySorter.Options()), thrown);
    fail();
  }

  @Test
  public void testSortTwice() throws Exception {
    SorterTestUtils.testSortTwice(InMemorySorter.create(new InMemorySorter.Options()), thrown);
    fail();
  }

  /** Verify an exception is thrown when the in memory sorter runs out of space. */
  @Test
  public void testOutOfSpace() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(is("No space remaining for in memory sorting"));
    SorterTestUtils.testRandom(
        () -> {
          InMemorySorter.Options options = new InMemorySorter.Options();
          options.setMemoryMB(1);
          return InMemorySorter.create(options);
        },
        1,
        10000000);
  }

  @Test
  public void testAddIfRoom() {
    InMemorySorter.Options options = new InMemorySorter.Options();
    options.setMemoryMB(1);
    InMemorySorter sorter = InMemorySorter.create(options);

    // Should be a few kb less than what the total buffer supports
    KV<byte[], byte[]> bigRecord = KV.of(new byte[1024 * 500], new byte[1024 * 500]);

    // First add should succeed, second add should fail due to insufficient room
    Assert.assertTrue(sorter.addIfRoom(bigRecord));
    Assert.assertFalse(sorter.addIfRoom(bigRecord));
  }

  @Test
  public void testAddIfRoomOverhead() {
    InMemorySorter.Options options = new InMemorySorter.Options();
    options.setMemoryMB(1);
    InMemorySorter sorter = InMemorySorter.create(options);

    // No bytes within record, should still run out of room due to memory overhead of record
    KV<byte[], byte[]> tinyRecord = KV.of(new byte[0], new byte[0]);

    // Verify we can't insert one million records into this one megabyte buffer
    boolean stillRoom = true;
    for (int i = 0; (i < 1000000) && stillRoom; i++) {
      stillRoom = sorter.addIfRoom(tinyRecord);
    }

    Assert.assertFalse(stillRoom);
  }

  @Test
  public void testNegativeMemory() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("memoryMB must be greater than zero");
    InMemorySorter.Options options = new InMemorySorter.Options();
    options.setMemoryMB(-1);
  }

  @Test
  public void testZeroMemory() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("memoryMB must be greater than zero");
    InMemorySorter.Options options = new InMemorySorter.Options();
    options.setMemoryMB(0);
  }
}
