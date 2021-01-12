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
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Distinct;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for the {@link Distinct} operator. */
@RunWith(JUnit4.class)
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
          protected PCollection<Integer> getOutput(PCollection<Integer> input) {
            return Distinct.of(input).output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
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
          protected PCollection<Integer> getOutput(PCollection<KV<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            PCollection<KV<Integer, Long>> distinct =
                Distinct.of(input)
                    .projected(KV::getKey)
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();
            return MapElements.of(distinct).using(KV::getKey).output();
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

          @Override
          protected TypeDescriptor<KV<Integer, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.longs());
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
          protected PCollection<Integer> getOutput(PCollection<KV<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            PCollection<KV<Integer, Long>> distinct =
                Distinct.of(input)
                    .projected(KV::getKey)
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();
            return MapElements.of(distinct).using(KV::getKey).output();
          }

          @Override
          protected List<KV<Integer, Long>> getInput() {
            List<KV<Integer, Long>> first = asTimedList(100, 1, 2, 3, 3, 2, 1);
            first.addAll(asTimedList(100, 1, 2, 3, 3, 2, 1));
            return first;
          }

          @Override
          protected TypeDescriptor<KV<Integer, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.longs());
          }
        });
  }

  @Test
  public void testSimpleDuplicatesWithStreamStrategyOldest() {
    execute(
        new AbstractTestCase<KV<String, Long>, String>() {

          @Override
          public List<String> getUnorderedOutput() {
            return Arrays.asList("2", "1", "3");
          }

          @Override
          protected PCollection<String> getOutput(PCollection<KV<String, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            PCollection<KV<String, Long>> distinct =
                Distinct.of(input)
                    .projected(
                        in -> in.getKey().substring(0, 1),
                        Distinct.SelectionPolicy.OLDEST,
                        TypeDescriptors.strings())
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();
            return MapElements.of(distinct).using(KV::getKey).output();
          }

          @Override
          protected List<KV<String, Long>> getInput() {
            return asTimedList(100, "1", "2", "3", "3.", "2.", "1.");
          }

          @Override
          protected TypeDescriptor<KV<String, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs());
          }
        });
  }

  @Test
  public void testSimpleDuplicatesWithStreamStrategyNewest() {
    execute(
        new AbstractTestCase<KV<String, Long>, String>() {

          @Override
          public List<String> getUnorderedOutput() {
            return Arrays.asList("2.", "1.", "3.");
          }

          @Override
          protected PCollection<String> getOutput(PCollection<KV<String, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            PCollection<KV<String, Long>> distinct =
                Distinct.of(input)
                    .projected(
                        in -> in.getKey().substring(0, 1),
                        Distinct.SelectionPolicy.NEWEST,
                        TypeDescriptors.strings())
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();
            return MapElements.of(distinct).using(KV::getKey).output();
          }

          @Override
          protected List<KV<String, Long>> getInput() {
            return asTimedList(100, "1", "2", "3", "3.", "2.", "1.");
          }

          @Override
          protected TypeDescriptor<KV<String, Long>> getInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs());
          }
        });
  }

  @SafeVarargs
  final <T> List<KV<T, Long>> asTimedList(long step, T... values) {
    final List<KV<T, Long>> ret = new ArrayList<>(values.length);
    long i = step;
    for (T v : values) {
      ret.add(KV.of(v, i));
      i += step;
    }
    return ret;
  }
}
