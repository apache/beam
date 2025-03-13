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
package org.apache.beam.examples.cookbook;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Distinct}. */
@RunWith(JUnit4.class)
public class DistinctExampleTest {

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testDistinct() {
    List<String> strings = Arrays.asList("k1", "k5", "k5", "k2", "k1", "k2", "k3");

    PCollection<String> input = p.apply(Create.of(strings).withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(Distinct.create());

    PAssert.that(output).containsInAnyOrder("k1", "k5", "k2", "k3");
    p.run().waitUntilFinish();
  }

  @Test
  public void testDistinctEmpty() {
    List<String> strings = Arrays.asList();

    PCollection<String> input = p.apply(Create.of(strings).withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(Distinct.create());

    PAssert.that(output).empty();
    p.run().waitUntilFinish();
  }
}
