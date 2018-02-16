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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineIntegerFn;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link CombineTranslation}. */
@RunWith(Enclosed.class)
public class CombineTranslationTest {

  /** Tests that simple {@link CombineFn CombineFns} can be translated to and from proto. */
  @RunWith(Parameterized.class)
  public static class TranslateSimpleCombinesTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Combine.CombineFn<Integer, ?, ?>> params() {
      BinaryCombineIntegerFn sum = Sum.ofIntegers();
      CombineFn<Integer, ?, Long> count = Count.combineFn();
      TestCombineFn test = new TestCombineFn();
      return ImmutableList.<CombineFn<Integer, ?, ?>>builder()
          .add(sum)
          .add(count)
          .add(test)
          .build();
    }

    @Rule public TestPipeline pipeline = TestPipeline.create();

    @Parameter(0)
    public Combine.CombineFn<Integer, ?, ?> combineFn;

    @Test
    public void testToFromProto() throws Exception {
      PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3));
      input.apply(Combine.globally(combineFn));
      final AtomicReference<AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>>> combine =
          new AtomicReference<>();
      pipeline.traverseTopologically(
          new PipelineVisitor.Defaults() {
            @Override
            public void leaveCompositeTransform(Node node) {
              if (node.getTransform() instanceof Combine.PerKey) {
                checkState(combine.get() == null);
                combine.set((AppliedPTransform) node.toAppliedPTransform(getPipeline()));
              }
            }
          });
      checkState(combine.get() != null);
      assertEquals(combineFn, CombineTranslation.getCombineFn(combine.get()));

      SdkComponents sdkComponents = SdkComponents.create();
      CombinePayload combineProto = CombineTranslation.toProto(combine.get(), sdkComponents);
      RunnerApi.Components componentsProto = sdkComponents.toComponents();

      assertEquals(
          combineFn.getAccumulatorCoder(pipeline.getCoderRegistry(), input.getCoder()),
          CombineTranslation.getAccumulatorCoder(
              combineProto, RehydratedComponents.forComponents(componentsProto)));
      assertEquals(combineFn, CombineTranslation.getCombineFn(combineProto));
    }
  }

  /** Tests that a {@link CombineFnWithContext} can be translated. */
  @RunWith(JUnit4.class)
  public static class ValidateCombineWithContextTest {
    @Rule public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testToFromProtoWithSideInputs() throws Exception {
      PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3));
      final PCollectionView<Iterable<String>> sideInput =
          pipeline.apply(Create.of("foo")).apply(View.asIterable());
      CombineFnWithContext<Integer, int[], Integer> combineFn = new TestCombineFnWithContext();
      input.apply(Combine.globally(combineFn).withSideInputs(sideInput).withoutDefaults());
      final AtomicReference<AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>>> combine =
          new AtomicReference<>();
      pipeline.traverseTopologically(
          new PipelineVisitor.Defaults() {
            @Override
            public void leaveCompositeTransform(Node node) {
              if (node.getTransform() instanceof Combine.PerKey) {
                checkState(combine.get() == null);
                combine.set((AppliedPTransform) node.toAppliedPTransform(getPipeline()));
              }
            }
          });
      checkState(combine.get() != null);
      assertEquals(combineFn, CombineTranslation.getCombineFn(combine.get()));

      SdkComponents sdkComponents = SdkComponents.create();
      CombinePayload combineProto = CombineTranslation.toProto(combine.get(), sdkComponents);
      RunnerApi.Components componentsProto = sdkComponents.toComponents();

      assertEquals(
          combineFn.getAccumulatorCoder(pipeline.getCoderRegistry(), input.getCoder()),
          CombineTranslation.getAccumulatorCoder(
              combineProto, RehydratedComponents.forComponents(componentsProto)));
      assertEquals(combineFn, CombineTranslation.getCombineFn(combineProto));
    }
  }

  private static class TestCombineFn extends Combine.CombineFn<Integer, Void, Void> {
    @Override
    public Void createAccumulator() {
      return null;
    }

    @Override
    public Coder<Void> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return (Coder) VoidCoder.of();
    }

    @Override
    public Void extractOutput(Void accumulator) {
      return accumulator;
    }

    @Override
    public Void mergeAccumulators(Iterable<Void> accumulators) {
      return null;
    }

    @Override
    public Void addInput(Void accumulator, Integer input) {
      return accumulator;
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other.getClass().equals(TestCombineFn.class);
    }

    @Override
    public int hashCode() {
      return TestCombineFn.class.hashCode();
    }
  }

  private static class TestCombineFnWithContext
      extends CombineFnWithContext<Integer, int[], Integer> {

    @Override
    public int[] createAccumulator(Context c) {
      return new int[1];
    }

    @Override
    public int[] addInput(int[] accumulator, Integer input, Context c) {
      accumulator[0] += input;
      return accumulator;
    }

    @Override
    public int[] mergeAccumulators(Iterable<int[]> accumulators, Context c) {
      int[] res = new int[1];
      for (int[] accum : accumulators) {
        res[0] += accum[0];
      }
      return res;
    }

    @Override
    public Integer extractOutput(int[] accumulator, Context c) {
      return accumulator[0];
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof TestCombineFnWithContext;
    }

    @Override
    public int hashCode() {
      return TestCombineFnWithContext.class.hashCode();
    }
  }
}
