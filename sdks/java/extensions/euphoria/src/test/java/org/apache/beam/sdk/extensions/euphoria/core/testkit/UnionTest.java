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
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for operator {@code Union}. */
@RunWith(JUnit4.class)
public class UnionTest extends AbstractOperatorTest {

  private static PCollection<Integer> createDataset(Pipeline pipeline, Integer... data) {
    return pipeline
        .apply("create-" + UUID.randomUUID(), Create.of(Arrays.asList(data)))
        .setTypeDescriptor(TypeDescriptors.integers());
  }

  @Test
  public void testUnion() {
    execute(
        new TestCase<Integer>() {

          @Override
          public PCollection<Integer> getOutput(Pipeline pipeline) {
            final PCollection<Integer> first = createDataset(pipeline, 1, 2, 3, 4, 5, 6);
            final PCollection<Integer> second = createDataset(pipeline, 7, 8, 9, 10, 11, 12);
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
          public PCollection<Integer> getOutput(Pipeline pipeline) {
            final PCollection<Integer> first = createDataset(pipeline, 1, 2, 3, 4, 5, 6);
            final PCollection<Integer> second = createDataset(pipeline, 7, 8, 9, 10, 11, 12);
            final PCollection<Integer> third = createDataset(pipeline, 13, 14, 15, 16, 17, 18);
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
          public PCollection<Integer> getOutput(Pipeline pipeline) {
            final PCollection<Integer> first = createDataset(pipeline, 1, 2, 3);
            final PCollection<Integer> second = createDataset(pipeline, 4, 5, 6);
            final PCollection<Integer> third = createDataset(pipeline, 7, 8, 9);
            final PCollection<Integer> fourth = createDataset(pipeline, 10, 11, 12);
            final PCollection<Integer> fifth = createDataset(pipeline, 13, 14, 15);
            return Union.of(first, second, third, fourth, fifth).output();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
          }
        });
  }
}
