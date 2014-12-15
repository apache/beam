/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.CoreMatchers.isA;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;


import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Tests for {@link View}. See also {@link ParDoTest} which
 * provides additional coverage since views can only be
 * observed via {@link ParDo}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class ViewTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testSingletonSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Integer, ?> view = pipeline
        .apply(Create.of(47))
        .apply(View.<Integer>asSingleton());

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.sideInput(view));
              }
            }));

    DataflowAssert.that(output)
        .containsInAnyOrder(47, 47, 47);

    pipeline.run();
  }

  @Test
  public void testEmptySingletonSideInput() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Integer, ?> view = pipeline
        .apply(Create.<Integer>of())
        .setCoder(VarIntCoder.of())
        .apply(View.<Integer>asSingleton());

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.sideInput(view));
              }
            }));

    thrown.expect(RuntimeException.class);
    thrown.expectCause(isA(NoSuchElementException.class));
    thrown.expectMessage("Empty");
    thrown.expectMessage("PCollection");
    thrown.expectMessage("singleton");

    pipeline.run();
  }

  @Test
  public void testNonSingletonSideInput() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Integer, ?> view = pipeline
        .apply(Create.<Integer>of(1, 2, 3))
        .apply(View.<Integer>asSingleton());

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                c.output(c.sideInput(view));
              }
            }));

    thrown.expect(RuntimeException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("PCollection");
    thrown.expectMessage("more than one");
    thrown.expectMessage("singleton");

    pipeline.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testIterableSideInput() {
    Pipeline pipeline = TestPipeline.create();

    final PCollectionView<Iterable<Integer>, ?> view = pipeline
        .apply(Create.of(11, 13, 17, 23))
        .apply(View.<Integer>asIterable());

    PCollection<Integer> output = pipeline
        .apply(Create.of(29, 31))
        .apply(ParDo.withSideInputs(view).of(
            new DoFn<Integer, Integer>() {
              @Override
              public void processElement(ProcessContext c) {
                for (Integer i : c.sideInput(view)) {
                  c.output(i);
                }
              }
            }));

    DataflowAssert.that(output).containsInAnyOrder(
        11, 13, 17, 23,
        11, 13, 17, 23);

    pipeline.run();
  }
}
