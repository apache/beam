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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.SumByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.junit.Test;

/** Test operator {@code SumByKey}. */
@Processing(Processing.Type.ALL)
public class SumByKeyTest extends AbstractOperatorTest {

  @Test
  public void testSumByKey() {
    execute(
        new AbstractTestCase<Integer, Pair<Integer, Long>>() {
          @Override
          protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
            return SumByKey.of(input)
                .keyBy(e -> e % 2)
                .valueBy(e -> (long) e)
                .windowBy(FixedWindows.of(org.joda.time.Duration.standardSeconds(1)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
          }

          @Override
          public List<Pair<Integer, Long>> getUnorderedOutput() {
            return Arrays.asList(Pair.of(0, 20L), Pair.of(1, 25L));
          }
        });
  }
}
