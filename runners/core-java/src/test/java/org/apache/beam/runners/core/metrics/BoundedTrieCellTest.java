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

import java.util.Arrays;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
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
        ImmutableSet.of(
            Arrays.asList("a", "a", String.valueOf(false)),
            Arrays.asList("b", "d", String.valueOf(false)),
            Arrays.asList("b", "c", String.valueOf(false))),
        cumulative.getResult());

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));

    // adding new value changes the cell
    cell.add("b", "a");
    BoundedTrieData newCumulative = cell.getCumulative();
    assertEquals(
        newCumulative.getResult(),
        ImmutableSet.of(
            Arrays.asList("a", "a", String.valueOf(false)),
            Arrays.asList("b", "d", String.valueOf(false)),
            Arrays.asList("b", "c", String.valueOf(false)),
            Arrays.asList("b", "a", String.valueOf(false))));

    // but not previously obtained cumulative value
    assertEquals(
        cumulative.getResult(),
        ImmutableSet.of(
            Arrays.asList("a", "a", String.valueOf(false)),
            Arrays.asList("b", "d", String.valueOf(false)),
            Arrays.asList("b", "c", String.valueOf(false))));

    assertThat(
        "Adding a new value made the cell dirty", cell.getDirty().beforeCommit(), equalTo(true));
  }
  // TODO:Add more tests
}
