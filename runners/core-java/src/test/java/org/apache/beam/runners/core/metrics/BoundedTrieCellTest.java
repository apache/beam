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
package org.apache.beam.runners.core.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.metrics.BoundedTrieResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link BoundedTrieCell}. */
public class BoundedTrieCellTest {
  private final BoundedTrieCell cell = new BoundedTrieCell(MetricName.named("lineage", "sources"));

  @Test
  public void testDeltaAndCumulative() {
    cell.add("a");
    cell.add("b", "c");
    cell.add("b", "d");
    cell.add("a", "a");
    BoundedTrieData cumulative = cell.getCumulative();
    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", "a", String.valueOf(false)),
                Arrays.asList("b", "d", String.valueOf(false)),
                Arrays.asList("b", "c", String.valueOf(false)))),
        cumulative.extractResult());

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));

    // adding new value changes the cell
    cell.add("b", "a");
    BoundedTrieData newCumulative = cell.getCumulative();
    assertEquals(
        newCumulative.extractResult(),
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", "a", String.valueOf(false)),
                Arrays.asList("b", "d", String.valueOf(false)),
                Arrays.asList("b", "c", String.valueOf(false)),
                Arrays.asList("b", "a", String.valueOf(false)))));

    // but not previously obtained cumulative value
    assertEquals(
        cumulative.extractResult(),
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", "a", String.valueOf(false)),
                Arrays.asList("b", "d", String.valueOf(false)),
                Arrays.asList("b", "c", String.valueOf(false)))));

    assertThat(
        "Adding a new value made the cell dirty", cell.getDirty().beforeCommit(), equalTo(true));
  }

  @Test
  public void testEquals() {
    BoundedTrieCell boundedTrieCell = new BoundedTrieCell(MetricName.named("namespace", "name"));
    BoundedTrieCell equal = new BoundedTrieCell(MetricName.named("namespace", "name"));
    assertEquals(boundedTrieCell, equal);
    assertEquals(boundedTrieCell.hashCode(), equal.hashCode());
  }

  @Test
  public void testNotEquals() {
    BoundedTrieCell boundedTrieCell = new BoundedTrieCell(MetricName.named("namespace", "name"));

    Assert.assertNotEquals(boundedTrieCell, new Object());

    BoundedTrieCell differentDirty = new BoundedTrieCell(MetricName.named("namespace", "name"));
    differentDirty.getDirty().afterModification();
    Assert.assertNotEquals(boundedTrieCell, differentDirty);
    Assert.assertNotEquals(boundedTrieCell.hashCode(), differentDirty.hashCode());

    BoundedTrieCell differentTrieCell = new BoundedTrieCell(MetricName.named("namespace", "name"));
    BoundedTrieData updateData = new BoundedTrieData(ImmutableList.of("hello"));
    differentTrieCell.update(updateData);
    Assert.assertNotEquals(boundedTrieCell, differentTrieCell);
    Assert.assertNotEquals(boundedTrieCell.hashCode(), differentTrieCell.hashCode());

    BoundedTrieCell differentName = new BoundedTrieCell(MetricName.named("DIFFERENT", "DIFFERENT"));
    Assert.assertNotEquals(boundedTrieCell, differentName);
    Assert.assertNotEquals(boundedTrieCell.hashCode(), differentName.hashCode());
  }

  @Test
  public void testReset() {
    BoundedTrieCell boundedTrieCell = new BoundedTrieCell(MetricName.named("namespace", "name"));
    boundedTrieCell.add("hello");
    Assert.assertNotEquals(boundedTrieCell.getDirty(), new DirtyState());
    assertThat(
        boundedTrieCell.getCumulative(), equalTo(new BoundedTrieData(ImmutableList.of("hello"))));

    boundedTrieCell.reset();
    assertThat(boundedTrieCell.getCumulative(), equalTo(new BoundedTrieData()));
    assertThat(boundedTrieCell.getDirty(), equalTo(new DirtyState()));
  }

  @Test(timeout = 5000)
  public void testBoundedTrieDiffrentThreadWrite() throws InterruptedException {
    BoundedTrieCell cell = new BoundedTrieCell(MetricName.named("namespace", "name"));
    AtomicBoolean finished = new AtomicBoolean(false);
    Thread increment =
        new Thread(
            () -> {
              for (long i = 0; !finished.get(); ++i) {
                cell.add(String.valueOf(i));
                try {
                  Thread.sleep(1);
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
    increment.start();
    Instant start = Instant.now();
    try {
      while (true) {
        BoundedTrieData cumulative = cell.getCumulative();
        if (Instant.now().isAfter(start.plusSeconds(3)) && cumulative.size() > 0) {
          finished.compareAndSet(false, true);
          break;
        }
      }
    } finally {
      increment.interrupt();
      increment.join();
    }

    BoundedTrieData s = cell.getCumulative();
    for (long i = 0; i < s.size(); ++i) {
      assertTrue(s.contains(ImmutableList.of(String.valueOf(i))));
    }
  }
}
