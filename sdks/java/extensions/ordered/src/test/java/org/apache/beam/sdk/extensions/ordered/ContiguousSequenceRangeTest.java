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
package org.apache.beam.sdk.extensions.ordered;

import java.util.Arrays;
import java.util.Collections;
import junit.framework.TestCase;
import org.joda.time.Instant;

public class ContiguousSequenceRangeTest extends TestCase {

  public void testCompareTo() {
    ContiguousSequenceRange refToEmpty = ContiguousSequenceRange.EMPTY;
    assertEquals("Empty ranges are equal", 0, ContiguousSequenceRange.EMPTY.compareTo(refToEmpty));

    assertEquals(
        "Empty range is smaller than another",
        -1,
        ContiguousSequenceRange.EMPTY.compareTo(ContiguousSequenceRange.of(0, 5, new Instant())));

    assertEquals(
        "First range is smaller than the second",
        -1,
        ContiguousSequenceRange.of(0, 2, new Instant())
            .compareTo(ContiguousSequenceRange.of(0, 5, new Instant())));

    assertEquals(
        "First range is larger than the second",
        1,
        ContiguousSequenceRange.of(0, 10, new Instant())
            .compareTo(ContiguousSequenceRange.of(0, 5, new Instant())));

    assertEquals(
        "Ranges are equal",
        0,
        ContiguousSequenceRange.of(0, 10, new Instant())
            .compareTo(ContiguousSequenceRange.of(0, 10, new Instant())));
  }

  public void testLargestRange() {
    assertEquals(
        "Empty if no elements",
        ContiguousSequenceRange.EMPTY,
        ContiguousSequenceRange.largestRange(Collections.EMPTY_LIST));

    ContiguousSequenceRange one = ContiguousSequenceRange.EMPTY;
    ContiguousSequenceRange two = ContiguousSequenceRange.of(0, 5, new Instant());
    ContiguousSequenceRange three = ContiguousSequenceRange.of(0, 22, new Instant());
    ContiguousSequenceRange four = ContiguousSequenceRange.of(0, 10, new Instant());
    assertEquals(
        "third range",
        three,
        ContiguousSequenceRange.largestRange(
            Arrays.asList(new ContiguousSequenceRange[] {one, two, three, four})));
  }
}
