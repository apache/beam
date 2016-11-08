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

import static org.junit.Assert.fail;

import org.apache.beam.sdk.extensions.sorter.SorterTestUtils.SorterGenerator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Sorter. */
@RunWith(JUnit4.class)
public class ExternalSorterTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEmpty() throws Exception {
    SorterTestUtils.testEmpty(ExternalSorter.create(new ExternalSorter.Options()));
  }

  @Test
  public void testSingleElement() throws Exception {
    SorterTestUtils.testSingleElement(ExternalSorter.create(new ExternalSorter.Options()));
  }

  @Test
  public void testEmptyKeyValueElement() throws Exception {
    SorterTestUtils.testEmptyKeyValueElement(ExternalSorter.create(new ExternalSorter.Options()));
  }

  @Test
  public void testMultipleIterations() throws Exception {
    SorterTestUtils.testMultipleIterations(ExternalSorter.create(new ExternalSorter.Options()));
  }

  @Test
  public void testRandom() throws Exception {
    SorterTestUtils.testRandom(
        new SorterGenerator() {
          @Override
          public Sorter generateSorter() throws Exception {
            return ExternalSorter.create(new ExternalSorter.Options());
          }
        },
        1,
        1000000);
  }

  @Test
  public void testAddAfterSort() throws Exception {
    SorterTestUtils.testAddAfterSort(ExternalSorter.create(new ExternalSorter.Options()), thrown);
    fail();
  }

  @Test
  public void testSortTwice() throws Exception {
    SorterTestUtils.testSortTwice(ExternalSorter.create(new ExternalSorter.Options()), thrown);
    fail();
  }

  @Test
  public void testNegativeMemory() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("memoryMB must be greater than zero");
    ExternalSorter.Options options = new ExternalSorter.Options();
    options.setMemoryMB(-1);
  }
}
