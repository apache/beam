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

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.TestUtils.LINES;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests for First.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class FirstTest
    implements Serializable /* to allow anon inner classes */ {
  // PRE: lines contains no duplicates.
  void runTestFirst(final List<String> lines, int limit, boolean ordered) {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(lines))
        .setCoder(StringUtf8Coder.of());

    if (ordered) {
      input.setOrdered(true);
    }

    PCollection<String> output =
        input.apply(First.<String>of(limit));

    if (ordered) {
      output.setOrdered(true);
    }

    final int expectedSize = Math.min(limit, lines.size());
    if (ordered) {
      List<String> expected = lines.subList(0, expectedSize);
      if (expected.isEmpty()) {
        DataflowAssert.that(output)
            .containsInAnyOrder(expected);
      } else {
        DataflowAssert.that(output)
            .containsInOrder(expected);
      }
    } else {
      DataflowAssert.that(output)
          .satisfies(new SerializableFunction<Iterable<String>, Void>() {
              @Override
              public Void apply(Iterable<String> actualIter) {
                // Make sure actual is the right length, and is a
                // subset of expected.
                List<String> actual = new ArrayList<>();
                for (String s : actualIter) {
                  actual.add(s);
                }
                assertEquals(expectedSize, actual.size());
                Set<String> actualAsSet = new TreeSet<>(actual);
                Set<String> linesAsSet = new TreeSet<>(lines);
                assertEquals(actual.size(), actualAsSet.size());
                assertEquals(lines.size(), linesAsSet.size());
                assertTrue(linesAsSet.containsAll(actualAsSet));
                return null;
              }
            });
    }

    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testFirst() {
    runTestFirst(LINES, 0, false);
    runTestFirst(LINES, LINES.size() / 2, false);
    runTestFirst(LINES, LINES.size() * 2, false);
  }

  @Test
  // Extra tests, not worth the time to run on the real service.
  public void testFirstMore() {
    runTestFirst(LINES, LINES.size() - 1, false);
    runTestFirst(LINES, LINES.size(), false);
    runTestFirst(LINES, LINES.size() + 1, false);
  }

  // TODO: setOrdered(true) isn't supported yet by the Dataflow service.
  @Test
  public void testFirstOrdered() {
    runTestFirst(LINES, 0, true);
    runTestFirst(LINES, LINES.size() / 2, true);
    runTestFirst(LINES, LINES.size() - 1, true);
    runTestFirst(LINES, LINES.size(), true);
    runTestFirst(LINES, LINES.size() + 1, true);
    runTestFirst(LINES, LINES.size() * 2, true);
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testFirstEmpty() {
    runTestFirst(NO_LINES, 0, false);
    runTestFirst(NO_LINES, 1, false);
  }

  @Test
  // TODO: setOrdered(true) isn't supported yet by the Dataflow service.
  public void testFirstEmptyOrdered() {
    runTestFirst(NO_LINES, 0, true);
    runTestFirst(NO_LINES, 1, true);
  }
}
