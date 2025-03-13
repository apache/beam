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
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator {@code Filter}. */
@RunWith(JUnit4.class)
public class FilterTest extends AbstractOperatorTest {

  @Test
  public void testFilter() {
    execute(
        new AbstractTestCase<Integer, Integer>() {

          @Override
          protected PCollection<Integer> getOutput(PCollection<Integer> input) {
            return Filter.of(input).by(e -> e % 2 == 0).output();
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
          }

          @Override
          protected TypeDescriptor<Integer> getInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          public List<Integer> getUnorderedOutput() {
            return Arrays.asList(2, 4, 6, 8, 10, 12, 14);
          }
        });
  }
}
