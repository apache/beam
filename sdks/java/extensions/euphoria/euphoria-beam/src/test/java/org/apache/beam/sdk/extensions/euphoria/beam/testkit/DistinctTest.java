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
package org.apache.beam.sdk.extensions.euphoria.beam.testkit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.Processing.Type;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Distinct;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.junit.Test;

/** Test for the {@link Distinct} operator. */
@Processing(Type.ALL)
public class DistinctTest extends AbstractOperatorTest {

  /** Test simple duplicates. */
  @Test
  public void testSimpleDuplicatesWithNoWindowing() {
    execute(
        new AbstractTestCase<Integer, Integer>() {

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3);
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<Integer> input) {
            return Distinct.of(input).output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 3, 2, 1);
          }
        });
  }

  /** Test simple duplicates with unbounded input with count window. */
  @Test
  public void testSimpleDuplicatesWithTimeWindowing() {
    execute(
        new AbstractTestCase<Pair<Integer, Long>, Integer>() {

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 2, 1);
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(Pair::getSecond).output();
            return Distinct.of(input)
                .mapped(Pair::getFirst)
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Pair<Integer, Long>> getInput() {
            return Arrays.asList(
                Pair.of(1, 100L),
                Pair.of(2, 300L), // first window
                Pair.of(3, 1200L),
                Pair.of(3, 1500L), // second window
                Pair.of(2, 2200L),
                Pair.of(1, 2700L));
          }
        });
  }

  @Test
  public void testSimpleDuplicatesWithStream() {
    execute(
        new AbstractTestCase<Pair<Integer, Long>, Integer>() {

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(2, 1, 3);
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<Pair<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(Pair::getSecond).output();
            return Distinct.of(input)
                .mapped(Pair::getFirst)
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Pair<Integer, Long>> getInput() {
            List<Pair<Integer, Long>> first = asTimedList(100, 1, 2, 3, 3, 2, 1);
            first.addAll(asTimedList(100, 1, 2, 3, 3, 2, 1));
            return first;
          }
        });
  }

  private List<Pair<Integer, Long>> asTimedList(long step, Integer... values) {
    List<Pair<Integer, Long>> ret = new ArrayList<>(values.length);
    long i = step;
    for (Integer v : values) {
      ret.add(Pair.of(v, i));
      i += step;
    }
    return ret;
  }
}
