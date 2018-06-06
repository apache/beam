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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Time;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceWindow;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.junit.Test;

/** Test operator {@code ReduceByKey}. */
@Processing(Processing.Type.ALL)
public class ReduceWindowTest extends AbstractOperatorTest {

  @Test
  public void testReduceWithWindowing() {
    execute(
        new AbstractTestCase<Integer, Integer>() {
          @Override
          protected Dataset<Integer> getOutput(Dataset<Integer> input) {
            Dataset<Integer> withEventTime =
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
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(55);
          }
        });
  }

  @Test
  public void testReduceWithAttachedWindowing() {
    execute(
        new AbstractTestCase<Integer, Integer>() {
          @Override
          protected Dataset<Integer> getOutput(Dataset<Integer> input) {
            Dataset<Integer> withEventTime =
                AssignEventTime.of(input).using(i -> 1000L * i).output();

            Dataset<Integer> first =
                ReduceWindow.of(withEventTime)
                    .combineBy(Sums.ofInts())
                    .windowBy(FixedWindows.of(org.joda.time.Duration.standardHours(1)))
                    .triggeredBy(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .output();

            return ReduceWindow.of(first).combineBy(Sums.ofInts()).output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(55);
          }
        });
  }
}
