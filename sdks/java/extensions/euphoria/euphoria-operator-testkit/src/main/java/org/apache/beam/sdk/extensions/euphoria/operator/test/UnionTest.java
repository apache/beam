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
package org.apache.beam.sdk.extensions.euphoria.operator.test;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Test for operator {@code Union}. */
@Processing(Processing.Type.ALL)
public class UnionTest extends AbstractOperatorTest {

  @Test
  public void testUnion() {
    execute(
        new TestCase<Integer>() {

          @Override
          public Dataset<Integer> getOutput(Flow flow, boolean bounded) {
            final Dataset<Integer> first =
                flow.createInput(
                    ListDataSource.of(bounded, Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6)));

            final Dataset<Integer> second =
                flow.createInput(
                    ListDataSource.of(bounded, Arrays.asList(7, 8, 9), Arrays.asList(10, 11, 12)));

            return Union.of(first, second).output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
          }
        });
  }

  @Test
  public void testUnion_threeDataSets() {
    execute(
        new TestCase<Integer>() {

          @Override
          public Dataset<Integer> getOutput(Flow flow, boolean bounded) {
            final Dataset<Integer> first =
                flow.createInput(
                    ListDataSource.of(bounded, Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6)));

            final Dataset<Integer> second =
                flow.createInput(
                    ListDataSource.of(bounded, Arrays.asList(7, 8, 9), Arrays.asList(10, 11, 12)));

            final Dataset<Integer> third =
                flow.createInput(
                    ListDataSource.of(
                        bounded, Arrays.asList(13, 14, 15), Arrays.asList(16, 17, 18)));

            return Union.of(first, second, third).output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18);
          }
        });
  }

  @Test
  public void testUnion_threeDataSets_differentNumberOfPartitions() {
    execute(
        new TestCase<Integer>() {

          @Override
          public Dataset<Integer> getOutput(Flow flow, boolean bounded) {
            final Dataset<Integer> first =
                flow.createInput(ListDataSource.of(bounded, Arrays.asList(1, 2, 3)));

            final Dataset<Integer> second =
                flow.createInput(
                    ListDataSource.of(bounded, Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9)));

            final Dataset<Integer> third =
                flow.createInput(
                    ListDataSource.of(
                        bounded,
                        Arrays.asList(10, 11, 12),
                        Arrays.asList(13, 14, 15),
                        Arrays.asList(16, 17, 18)));

            return Union.of(first, second, third).output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18);
          }
        });
  }

  @Test
  public void testUnion_fiveDataSets() {
    execute(
        new TestCase<Integer>() {

          @Override
          public Dataset<Integer> getOutput(Flow flow, boolean bounded) {
            final Dataset<Integer> first =
                flow.createInput(ListDataSource.of(bounded, Arrays.asList(1, 2, 3)));

            final Dataset<Integer> second =
                flow.createInput(ListDataSource.of(bounded, Arrays.asList(4, 5, 6)));

            final Dataset<Integer> third =
                flow.createInput(ListDataSource.of(bounded, Arrays.asList(7, 8, 9)));

            final Dataset<Integer> fourth =
                flow.createInput(ListDataSource.of(bounded, Arrays.asList(10, 11, 12)));

            final Dataset<Integer> fifth =
                flow.createInput(ListDataSource.of(bounded, Arrays.asList(13, 14, 15)));

            return Union.of(first, second, third, fourth, fifth).output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
          }
        });
  }
}
