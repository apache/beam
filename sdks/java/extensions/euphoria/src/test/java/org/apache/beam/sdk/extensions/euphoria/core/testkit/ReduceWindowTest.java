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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceWindow;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator {@code ReduceByKey}. */
@RunWith(JUnit4.class)
public class ReduceWindowTest extends AbstractOperatorTest {

  @Test
  public void testReduceWithWindowing() {
    execute(
        new AbstractTestCase<Integer, Integer>() {
          @Override
          protected PCollection<Integer> getOutput(PCollection<Integer> input) {
            PCollection<Integer> withEventTime =
                AssignEventTime.of(input).using(i -> 1000L * i).output();

            return ReduceWindow.of(withEventTime)
                .combineBy(Sums.ofInts())
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Collections.singletonList(55);
          }
        });
  }

  @Test
  public void testReduceWithAttachedWindowing() {
    execute(
        new AbstractTestCase<Integer, Integer>() {
          @Override
          protected PCollection<Integer> getOutput(PCollection<Integer> input) {
            PCollection<Integer> withEventTime =
                AssignEventTime.of(input).using(i -> 1000L * i).output();

            PCollection<Integer> first =
                ReduceWindow.named("first-reduce")
                    .of(withEventTime)
                    .combineBy(Sums.ofInts())
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();

            return ReduceWindow.named("second-reduce").of(first).combineBy(Sums.ofInts()).output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Collections.singletonList(55);
          }
        });
  }

  @Test
  public void testReduceWithAttachedWindowingMoreWindows() {
    execute(
        new AbstractTestCase<Integer, Integer>() {
          @Override
          protected PCollection<Integer> getOutput(PCollection<Integer> input) {
            PCollection<Integer> withEventTime =
                AssignEventTime.of(input).using(i -> 1000L * i).output();

            PCollection<Integer> first =
                ReduceWindow.named("first-reduce")
                    .of(withEventTime)
                    .combineBy(Sums.ofInts())
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(5)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();

            return ReduceWindow.named("second-reduce").of(first).combineBy(Sums.ofInts()).output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(10, 35, 10, 100);
          }
        });
  }
}
