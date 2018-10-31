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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Test operator {@code FlatMap}. */
public class FlatMapTest extends AbstractOperatorTest {

  @Test
  public void testExplodeOnTwoPartitions() {
    execute(
        new AbstractTestCase<Integer, Integer>() {

          @Override
          protected Dataset<Integer> getOutput(Dataset<Integer> input) {
            return FlatMap.of(input)
                .using(
                    (Integer e, Collector<Integer> c) -> {
                      for (int i = 1; i <= e; i++) {
                        c.collect(i);
                      }
                    })
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 1, 2, 1);
          }
        });
  }

  @Test
  public void testCounterTest() {
    execute(
        new AbstractTestCase<Integer, Integer>() {

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 0, 10, 20);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected Dataset<Integer> getOutput(Dataset<Integer> input) {
            return FlatMap.named("test")
                .of(input)
                .using(
                    (UnaryFunctor<Integer, Integer>)
                        (elem, collector) -> {
                          collector.getCounter("input").increment();
                          collector.getCounter("sum").increment(elem);
                          collector.collect(elem * elem);
                        })
                .output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 4, 9, 16, 25, 36, 0, 100, 400);
          }

          @Override
          public void validateAccumulators(SnapshotProvider snapshots) {
            Map<String, Long> counters = snapshots.getCounterSnapshots();
            assertEquals(Long.valueOf(9L), counters.get("input"));
            assertEquals(Long.valueOf(51L), counters.get("sum"));
          }
        });
  }
}
