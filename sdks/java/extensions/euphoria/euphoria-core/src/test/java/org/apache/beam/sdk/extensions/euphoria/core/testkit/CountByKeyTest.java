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
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing.Type;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.junit.Test;

/** Test operator {@code CountByKey}. */
@Processing(Type.ALL)
public class CountByKeyTest extends AbstractOperatorTest {

  @Test
  public void testCount() {
    execute(
        new AbstractTestCase<Integer, KV<Integer, Long>>() {
          @Override
          protected Dataset<KV<Integer, Long>> getOutput(Dataset<Integer> input) {
            // ~ use stable event-time watermark
            input = AssignEventTime.of(input).using(e -> 0).output();
            return CountByKey.of(input)
                .keyBy(e -> e)
                .windowBy(FixedWindows.of(Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 10, 9, 8, 7, 6, 5, 4);
          }

          @Override
          public List<KV<Integer, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(2, 1L),
                KV.of(4, 2L),
                KV.of(6, 2L),
                KV.of(8, 1L),
                KV.of(10, 1L),
                KV.of(1, 1L),
                KV.of(3, 1L),
                KV.of(5, 2L),
                KV.of(7, 2L),
                KV.of(9, 1L));
          }
        });
  }

  @Test
  public void testWithEventTimeWindow() {
    execute(
        new AbstractTestCase<KV<Integer, Long>, KV<Integer, Long>>() {
          @Override
          protected Dataset<KV<Integer, Long>> getOutput(Dataset<KV<Integer, Long>> input) {
            input = AssignEventTime.of(input).using(KV::getValue).output();
            return CountByKey.of(input)
                .keyBy(KV::getKey)
                .windowBy(FixedWindows.of(Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<KV<Integer, Long>> getInput() {
            return Arrays.asList(
                KV.of(1, 200L),
                KV.of(2, 500L),
                KV.of(1, 800L),
                KV.of(3, 1400L),
                KV.of(3, 1200L),
                KV.of(4, 1800L),
                KV.of(5, 2100L),
                KV.of(5, 2300L),
                KV.of(5, 2700L),
                KV.of(5, 3500L),
                KV.of(5, 3300L),
                KV.of(6, 3800L),
                KV.of(7, 4400L),
                KV.of(7, 4500L),
                KV.of(10, 4600L),
                KV.of(10, 5100L),
                KV.of(9, 5200L),
                KV.of(9, 5500L),
                KV.of(9, 6300L),
                KV.of(9, 6700L));
          }

          @Override
          public List<KV<Integer, Long>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(1, 2L),
                KV.of(2, 1L),
                KV.of(3, 2L),
                KV.of(4, 1L),
                KV.of(5, 3L),
                KV.of(5, 2L),
                KV.of(6, 1L),
                KV.of(7, 2L),
                KV.of(10, 1L),
                KV.of(10, 1L),
                KV.of(9, 2L),
                KV.of(9, 2L));
          }
        });
  }
}
