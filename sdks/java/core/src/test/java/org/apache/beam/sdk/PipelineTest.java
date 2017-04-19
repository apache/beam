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
package org.apache.beam.sdk;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.CountingInput.BoundedCountingInput;
import org.apache.beam.sdk.io.CountingInput.UnboundedCountingInput;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Matchers;
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

  @Rule public final TestPipeline pipeline = TestPipeline.create();
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
  @Category(ValidatesRunner.class)
  public void testMultipleApply() {
    PTransform<PCollection<? extends String>, PCollection<String>> myTransform =
        addSuffix("+");

    PCollection<String> input = pipeline.apply(Create.<String>of(ImmutableList.of("a", "b")));

    PCollection<String> left = input.apply("Left1", myTransform).apply("Left2", myTransform);
    PCollection<String> right = input.apply("Right", myTransform);

    PCollection<String> both = PCollectionList.of(left).and(right)
        .apply(Flatten.<String>pCollections());

    PAssert.that(both).containsInAnyOrder("a++", "b++", "a+", "b+");

    pipeline.run();
  }

  private static PTransform<PCollection<? extends String>, PCollection<String>> addSuffix(
      final String suffix) {
    return MapElements.via(new SimpleFunction<String, String>() {
      @Override
      public String apply(String input) {
        return input + suffix;
      }
    });
  }

  @Test
  public void testToString() {
    PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    assertEquals("Pipeline#" + pipeline.hashCode(), pipeline.toString());
  }

  @Test
  public void testStableUniqueNameOff() {
    pipeline.enableAbandonedNodeEnforcement(false);

    pipeline.getOptions().setStableUniqueNames(CheckEnabled.OFF);

    pipeline.apply(Create.of(5, 6, 7));
    pipeline.apply(Create.of(5, 6, 7));

    logged.verifyNotLogged("does not have a stable unique name.");
  }

  @Test
  public void testStableUniqueNameWarning() {
    pipeline.enableAbandonedNodeEnforcement(false);

    pipeline.getOptions().setStableUniqueNames(CheckEnabled.WARNING);

    pipeline.apply(Create.of(5, 6, 7));
    pipeline.apply(Create.of(5, 6, 7));

    logged.verifyWarn("does not have a stable unique name.");
  }

  @Test
  public void testStableUniqueNameError() {
    pipeline.getOptions().setStableUniqueNames(CheckEnabled.ERROR);

    pipeline.apply(Create.of(5, 6, 7));

    thrown.expectMessage("does not have a stable unique name.");
    pipeline.apply(Create.of(5, 6, 7));
  }

  /**
   * Tests that Pipeline supports a pass-through identity function.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testIdentityTransform() throws Exception {

    PCollection<Integer> output = pipeline
        .apply(Create.<Integer>of(1, 2, 3, 4))
        .apply("IdentityTransform", new IdentityTransform<PCollection<Integer>>());

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class IdentityTransform<T extends PInput & POutput>
      extends PTransform<T, T> {
    @Override
    public T expand(T input) {
      return input;
    }
  }

  /**
   * Tests that Pipeline supports pulling an element out of a tuple as a transform.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testTupleProjectionTransform() throws Exception {
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
    public PCollection<T> expand(PCollectionTuple input) {
      return input.get(tag);
    }
  }

  /**
   * Tests that Pipeline supports putting an element into a tuple as a transform.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testTupleInjectionTransform() throws Exception {
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
    public PCollectionTuple expand(PCollection<T> input) {
      return PCollectionTuple.of(tag, input);
    }
  }

  /**
   * Tests that an empty pipeline runs.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testEmptyPipeline() throws Exception {
    pipeline.run();
  }

  @Test
  public void testReplaceAll() {
    pipeline.enableAbandonedNodeEnforcement(false);
    pipeline.apply(CountingInput.unbounded());
    pipeline.apply(CountingInput.upTo(100L));

    pipeline.replaceAll(
        ImmutableList.of(
            PTransformOverride.of(
                new PTransformMatcher() {
                  @Override
                  public boolean matches(AppliedPTransform<?, ?, ?> application) {
                    return application.getTransform() instanceof UnboundedCountingInput;
                  }
                },
                new UnboundedCountingInputOverride()),
            PTransformOverride.of(
                new PTransformMatcher() {
                  @Override
                  public boolean matches(AppliedPTransform<?, ?, ?> application) {
                    return application.getTransform() instanceof BoundedCountingInput;
                  }
                },
                new BoundedCountingInputOverride())));
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              assertThat(
                  node.getTransform().getClass(),
                  not(
                      anyOf(
                          Matchers.<Class<? extends PTransform>>equalTo(
                              UnboundedCountingInput.class),
                          Matchers.<Class<? extends PTransform>>equalTo(
                              BoundedCountingInput.class))));
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
  }

  /**
   * Tests that {@link Pipeline#replaceAll(List)} throws when one of the PTransformOverride still
   * matches.
   */
  @Test
  public void testReplaceAllIncomplete() {
    pipeline.enableAbandonedNodeEnforcement(false);
    pipeline.apply(CountingInput.unbounded());

    // The order is such that the output of the second will match the first, which is not permitted
    thrown.expect(IllegalStateException.class);
    pipeline.replaceAll(
        ImmutableList.of(
            PTransformOverride.of(
                new PTransformMatcher() {
                  @Override
                  public boolean matches(AppliedPTransform<?, ?, ?> application) {
                    return application.getTransform() instanceof BoundedCountingInput;
                  }
                },
                new BoundedCountingInputOverride()),
            PTransformOverride.of(
                new PTransformMatcher() {
                  @Override
                  public boolean matches(AppliedPTransform<?, ?, ?> application) {
                    return application.getTransform() instanceof UnboundedCountingInput;
                  }
                },
                new UnboundedCountingInputOverride())));
  }

  @Test
  public void testReplacedNames() {
    final PCollection<String> originalInput = pipeline.apply(Create.of("foo", "bar", "baz"));
    class OriginalTransform extends PTransform<PCollection<String>, PCollection<Long>> {
      @Override
      public PCollection<Long> expand(PCollection<String> input) {
        return input.apply("custom_name", Count.<String>globally());
      }
    }
    class ReplacementTransform extends PTransform<PCollection<String>, PCollection<Long>> {
      @Override
      public PCollection<Long> expand(PCollection<String> input) {
        return input.apply("custom_name", Count.<String>globally());
      }
    }
    class ReplacementOverrideFactory
        implements PTransformOverrideFactory<
            PCollection<String>, PCollection<Long>, OriginalTransform> {
      @Override
      public PTransformReplacement<PCollection<String>, PCollection<Long>> getReplacementTransform(
          AppliedPTransform<PCollection<String>, PCollection<Long>, OriginalTransform> transform) {
        return PTransformReplacement.of(originalInput, new ReplacementTransform());
      }

      @Override
      public Map<PValue, ReplacementOutput> mapOutputs(
          Map<TupleTag<?>, PValue> outputs, PCollection<Long> newOutput) {
        return Collections.<PValue, ReplacementOutput>singletonMap(
            newOutput,
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(
                    Iterables.getOnlyElement(outputs.values())),
                    TaggedPValue.ofExpandedValue(newOutput)));
      }
    }

    class OriginalMatcher implements PTransformMatcher {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        return application.getTransform() instanceof OriginalTransform;
      }
    }

    originalInput.apply("original_application", new OriginalTransform());
    pipeline.replaceAll(
        Collections.singletonList(
            PTransformOverride.of(new OriginalMatcher(), new ReplacementOverrideFactory())));
    final Set<String> names = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void leaveCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              names.add(node.getFullName());
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            names.add(node.getFullName());
          }
        });

    assertThat(names, hasItem("original_application/custom_name"));
    assertThat(names, not(hasItem("original_application/custom_name2")));
  }

  static class BoundedCountingInputOverride
      implements PTransformOverrideFactory<PBegin, PCollection<Long>, BoundedCountingInput> {
    @Override
    public PTransformReplacement<PBegin, PCollection<Long>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<Long>, BoundedCountingInput> transform) {
      return PTransformReplacement.of(transform.getPipeline().begin(), Create.of(0L));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<Long> newOutput) {
      Map.Entry<TupleTag<?>, PValue> original = Iterables.getOnlyElement(outputs.entrySet());
      Map.Entry<TupleTag<?>, PValue> replacement =
          Iterables.getOnlyElement(newOutput.expand().entrySet());
      return Collections.<PValue, ReplacementOutput>singletonMap(
          newOutput,
          ReplacementOutput.of(
              TaggedPValue.of(original.getKey(), original.getValue()),
              TaggedPValue.of(replacement.getKey(), replacement.getValue())));
    }
  }
  static class UnboundedCountingInputOverride
      implements PTransformOverrideFactory<PBegin, PCollection<Long>, UnboundedCountingInput> {

    @Override
    public PTransformReplacement<PBegin, PCollection<Long>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<Long>, UnboundedCountingInput> transform) {
      return PTransformReplacement.of(transform.getPipeline().begin(), CountingInput.upTo(100L));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<Long> newOutput) {
      Map.Entry<TupleTag<?>, PValue> original = Iterables.getOnlyElement(outputs.entrySet());
      Map.Entry<TupleTag<?>, PValue> replacement =
          Iterables.getOnlyElement(newOutput.expand().entrySet());
      return Collections.<PValue, ReplacementOutput>singletonMap(
          newOutput,
          ReplacementOutput.of(
              TaggedPValue.of(original.getKey(), original.getValue()),
              TaggedPValue.of(replacement.getKey(), replacement.getValue())));
    }
  }
}
