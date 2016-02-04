/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess.evaluator;

import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link InProcessCreate}.
 */
@RunWith(JUnit4.class)
public class InProcessCreateTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConvertsCreate() {
    TestPipeline p = TestPipeline.create();
    Create.Values<Integer> og = Create.of(1, 2, 3);

    InProcessCreate<Integer> converted = InProcessCreate.from(og);

    DataflowAssert.that(p.apply(converted)).containsInAnyOrder(2, 1, 3);
  }

  static class Record implements Serializable {}

  static class Record2 extends Record {}

  @Test
  public void testThrowsIllegalArgumentWhenCannotInferCoder() {
    Create.Values<Record> og = Create.of(new Record(), new Record2());
    InProcessCreate<Record> converted = InProcessCreate.from(og);

    Pipeline p = TestPipeline.create();

    // Create won't infer a default coder in this case.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(Matchers.containsString("Unable to infer a coder"));

    PCollection<Record> c = p.apply(converted);
    p.run();

    fail("Unexpectedly Inferred Coder " + c.getCoder());
  }
}
