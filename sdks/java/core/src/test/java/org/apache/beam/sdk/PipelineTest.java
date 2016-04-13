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
package com.google.cloud.dataflow.sdk;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.PAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Pipeline.
 */
@RunWith(JUnit4.class)
public class PipelineTest {

  @Rule public ExpectedLogs logged = ExpectedLogs.none(Pipeline.class);
  @Rule public ExpectedException thrown = ExpectedException.none();

  static class PipelineWrapper extends Pipeline {
    protected PipelineWrapper(PipelineRunner<?> runner) {
      super(runner, PipelineOptionsFactory.create());
    }
  }

  // Mock class that throws a user code exception during the call to
  // Pipeline.run().
  static class TestPipelineRunnerThrowingUserException
      extends PipelineRunner<PipelineResult> {
    @Override
    public PipelineResult run(Pipeline pipeline) {
      Throwable t = new IllegalStateException("user code exception");
      throw UserCodeException.wrap(t);
    }
  }

  // Mock class that throws an SDK or API client code exception during
  // the call to Pipeline.run().
  static class TestPipelineRunnerThrowingSDKException
      extends PipelineRunner<PipelineResult> {
    @Override
    public PipelineResult run(Pipeline pipeline) {
      throw new IllegalStateException("SDK exception");
    }
  }

  @Test
  public void testPipelineUserExceptionHandling() {
    Pipeline p = new PipelineWrapper(
        new TestPipelineRunnerThrowingUserException());

    // Check pipeline runner correctly catches user errors.
    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("user code exception");
    p.run();
  }

  @Test
  public void testPipelineSDKExceptionHandling() {
    Pipeline p = new PipelineWrapper(new TestPipelineRunnerThrowingSDKException());

    // Check pipeline runner correctly catches SDK errors.
    try {
      p.run();
      fail("Should have thrown an exception.");
    } catch (RuntimeException exn) {
      // Make sure the exception isn't a UserCodeException.
      Assert.assertThat(exn, not(instanceOf(UserCodeException.class)));
      // Assert that the message is correct.
      Assert.assertThat(exn.getMessage(), containsString("SDK exception"));
      // RuntimeException should be IllegalStateException.
      Assert.assertThat(exn, instanceOf(IllegalStateException.class));
    }
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testMultipleApply() {
    PTransform<PCollection<? extends String>, PCollection<String>> myTransform =
        addSuffix("+");

    Pipeline p = TestPipeline.create();
    PCollection<String> input = p.apply(Create.<String>of(ImmutableList.of("a", "b")));

    PCollection<String> left = input.apply("Left1", myTransform).apply("Left2", myTransform);
    PCollection<String> right = input.apply("Right", myTransform);

    PCollection<String> both = PCollectionList.of(left).and(right)
        .apply(Flatten.<String>pCollections());

    PAssert.that(both).containsInAnyOrder("a++", "b++", "a+", "b+");

    p.run();
  }

  private static PTransform<PCollection<? extends String>, PCollection<String>> addSuffix(
      final String suffix) {
    return ParDo.of(new DoFn<String, String>() {
      @Override
      public void processElement(DoFn<String, String>.ProcessContext c) {
        c.output(c.element() + suffix);
      }
    });
  }

  @Test
  public void testToString() {
    PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);
    options.setRunner(DirectPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    assertEquals("Pipeline#" + pipeline.hashCode(), pipeline.toString());
  }

  @Test
  public void testStableUniqueNameOff() {
    Pipeline p = TestPipeline.create();
    p.getOptions().setStableUniqueNames(CheckEnabled.OFF);

    p.apply(Create.of(5, 6, 7));
    p.apply(Create.of(5, 6, 7));

    logged.verifyNotLogged("does not have a stable unique name.");
  }

  @Test
  public void testStableUniqueNameWarning() {
    Pipeline p = TestPipeline.create();
    p.getOptions().setStableUniqueNames(CheckEnabled.WARNING);

    p.apply(Create.of(5, 6, 7));
    p.apply(Create.of(5, 6, 7));

    logged.verifyWarn("does not have a stable unique name.");
  }

  @Test
  public void testStableUniqueNameError() {
    Pipeline p = TestPipeline.create();
    p.getOptions().setStableUniqueNames(CheckEnabled.ERROR);

    p.apply(Create.of(5, 6, 7));

    thrown.expectMessage("does not have a stable unique name.");
    p.apply(Create.of(5, 6, 7));
  }

  /**
   * Tests that Pipeline supports a pass-through identity function.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testIdentityTransform() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> output = pipeline
        .apply(Create.<Integer>of(1, 2, 3, 4))
        .apply("IdentityTransform", new IdentityTransform<PCollection<Integer>>());

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class IdentityTransform<T extends PInput & POutput>
      extends PTransform<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }

  /**
   * Tests that Pipeline supports pulling an element out of a tuple as a transform.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testTupleProjectionTransform() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> input = pipeline
        .apply(Create.<Integer>of(1, 2, 3, 4));

    TupleTag<Integer> tag = new TupleTag<Integer>();
    PCollectionTuple tuple = PCollectionTuple.of(tag, input);

    PCollection<Integer> output = tuple
        .apply("ProjectTag", new TupleProjectionTransform<Integer>(tag));

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class TupleProjectionTransform<T>
      extends PTransform<PCollectionTuple, PCollection<T>> {
    private TupleTag<T> tag;

    public TupleProjectionTransform(TupleTag<T> tag) {
      this.tag = tag;
    }

    @Override
    public PCollection<T> apply(PCollectionTuple input) {
      return input.get(tag);
    }
  }

  /**
   * Tests that Pipeline supports putting an element into a tuple as a transform.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testTupleInjectionTransform() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> input = pipeline
        .apply(Create.<Integer>of(1, 2, 3, 4));

    TupleTag<Integer> tag = new TupleTag<Integer>();

    PCollectionTuple output = input
        .apply("ProjectTag", new TupleInjectionTransform<Integer>(tag));

    PAssert.that(output.get(tag)).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class TupleInjectionTransform<T>
      extends PTransform<PCollection<T>, PCollectionTuple> {
    private TupleTag<T> tag;

    public TupleInjectionTransform(TupleTag<T> tag) {
      this.tag = tag;
    }

    @Override
    public PCollectionTuple apply(PCollection<T> input) {
      return PCollectionTuple.of(tag, input);
    }
  }

  /**
   * Tests that an empty pipeline runs.
   */
  @Test
  public void testEmptyPipeline() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    pipeline.run();
  }
}
