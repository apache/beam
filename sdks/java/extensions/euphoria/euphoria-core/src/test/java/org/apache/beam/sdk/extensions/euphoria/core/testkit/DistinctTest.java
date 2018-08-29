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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Distinct;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing.Type;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/** Test for the {@link Distinct} operator. */
@Processing(Type.ALL)
public class DistinctTest extends AbstractOperatorTest {

  /** Test simple duplicates. */
  // Distinct operator with unbounded dataset without windowing do not work,
  // since it is translated into GroupByKey."
  @Processing(Processing.Type.BOUNDED)
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
        new AbstractTestCase<KV<Integer, Long>, Integer>() {

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 2, 1);
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<KV<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            return Distinct.of(input)
                .mapped(KV::getKey)
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<KV<Integer, Long>> getInput() {
            return Arrays.asList(
                KV.of(1, 100L),
                KV.of(2, 300L), // first window
                KV.of(3, 1200L),
                KV.of(3, 1500L), // second window
                KV.of(2, 2200L),
                KV.of(1, 2700L));
          }
        });
  }

  @Test
  public void testSimpleDuplicatesWithStream() {
    execute(
        new AbstractTestCase<KV<Integer, Long>, Integer>() {

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(2, 1, 3);
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<KV<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            return Distinct.of(input)
                .mapped(KV::getKey)
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<KV<Integer, Long>> getInput() {
            List<KV<Integer, Long>> first = asTimedList(100, 1, 2, 3, 3, 2, 1);
            first.addAll(asTimedList(100, 1, 2, 3, 3, 2, 1));
            return first;
          }
        });
  }

  private List<KV<Integer, Long>> asTimedList(long step, Integer... values) {
    List<KV<Integer, Long>> ret = new ArrayList<>(values.length);
    long i = step;
    for (Integer v : values) {
      ret.add(KV.of(v, i));
      i += step;
    }
    return ret;
  }
}
