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

package org.apache.beam.runners.direct;

import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ParDoMultiOverrideFactory}.
 */
@RunWith(JUnit4.class)
public class ParDoMultiOverrideFactoryTest {
  private ParDoMultiOverrideFactory factory = new ParDoMultiOverrideFactory();

  @Test
  public void getInputSucceeds() {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> input = p.apply(Create.of(1, 2, 3));
    PCollection<?> reconstructed = factory.getInput(input.expand(), p);
    assertThat(reconstructed, Matchers.<PCollection<?>>equalTo(input));
  }
}
