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
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableLikeCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code Flatten<T>} takes multiple {@code PCollection<T>}s bundled
 * into a {@code PCollectionList<T>} and returns a single
 * {@code PCollection<T>} containing all the elements in all the input
 * {@code PCollection}s.  The name "Flatten" suggests taking a list of
 * lists and flattening them into a single list.
 *
 * <p>Example of use:
 * <pre> {@code
 * PCollection<String> pc1 = ...;
 * PCollection<String> pc2 = ...;
 * PCollection<String> pc3 = ...;
 * PCollectionList<String> pcs = PCollectionList.of(pc1).and(pc2).and(pc3);
 * PCollection<String> merged = pcs.apply(Flatten.<String>pCollections());
 * } </pre>
 *
 * <p>By default, the {@code Coder} of the output {@code PCollection}
 * is the same as the {@code Coder} of the first {@code PCollection}
 * in the input {@code PCollectionList} (if the
 * {@code PCollectionList} is non-empty).
 *
 */
public class Flatten {

  /**
   * Returns a {@link PTransform} that flattens a {@link PCollectionList}
   * into a {@link PCollection} containing all the elements of all
   * the {@link PCollection}s in its input.
   *
   * <p>All inputs must have equal {@link WindowFn}s.
   * The output elements of {@code Flatten<T>} are in the same windows and
   * have the same timestamps as their corresponding input elements.  The output
   * {@code PCollection} will have the same
   * {@link WindowFn} as all of the inputs.
   *
   * @param <T> the type of the elements in the input and output
   * {@code PCollection}s.
   */
  public static <T> FlattenPCollectionList<T> pCollections() {
    return new FlattenPCollectionList<>();
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<Iterable<T>>}
   * and returns a {@code PCollection<T>} containing all the elements from
   * all the {@code Iterable}s.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<Iterable<Integer>> pcOfIterables = ...;
   * PCollection<Integer> pc = pcOfIterables.apply(Flatten.<Integer>iterables());
   * } </pre>
   *
   * <p>By default, the output {@code PCollection} encodes its elements
   * using the same {@code Coder} that the input uses for
   * the elements in its {@code Iterable}.
   *
   * @param <T> the type of the elements of the input {@code Iterable} and
   * the output {@code PCollection}
   */
  public static <T> FlattenIterables<T> iterables() {
    return new FlattenIterables<>();
  }

  /**
   * A {@link PTransform} that flattens a {@link PCollectionList}
   * into a {@link PCollection} containing all the elements of all
   * the {@link PCollection}s in its input.
   * Implements {@link #pCollections}.
   *
   * @param <T> the type of the elements in the input and output
   * {@code PCollection}s.
   */
  public static class FlattenPCollectionList<T>
      extends PTransform<PCollectionList<T>, PCollection<T>> {

    private FlattenPCollectionList() { }

    @Override
    public PCollection<T> apply(PCollectionList<T> inputs) {
      WindowingStrategy<?, ?> windowingStrategy;
      IsBounded isBounded = IsBounded.BOUNDED;
      if (!inputs.getAll().isEmpty()) {
        windowingStrategy = inputs.get(0).getWindowingStrategy();
        for (PCollection<?> input : inputs.getAll()) {
          WindowingStrategy<?, ?> other = input.getWindowingStrategy();
          if (!windowingStrategy.getWindowFn().isCompatible(other.getWindowFn())) {
            throw new IllegalStateException(
                "Inputs to Flatten had incompatible window windowFns: "
                + windowingStrategy.getWindowFn() + ", " + other.getWindowFn());
          }

          if (!windowingStrategy.getTrigger().getSpec()
              .isCompatible(other.getTrigger().getSpec())) {
            throw new IllegalStateException(
                "Inputs to Flatten had incompatible triggers: "
                + windowingStrategy.getTrigger() + ", " + other.getTrigger());
          }
          isBounded = isBounded.and(input.isBounded());
        }
      } else {
        windowingStrategy = WindowingStrategy.globalDefault();
      }

      return PCollection.<T>createPrimitiveOutputInternal(
          inputs.getPipeline(),
          windowingStrategy,
          isBounded);
    }

    @Override
    protected Coder<?> getDefaultOutputCoder(PCollectionList<T> input)
        throws CannotProvideCoderException {

      // Take coder from first collection
      for (PCollection<T> pCollection : input.getAll()) {
        return pCollection.getCoder();
      }

      // No inputs
      throw new CannotProvideCoderException(
          this.getClass().getSimpleName() + " cannot provide a Coder for"
          + " empty " + PCollectionList.class.getSimpleName());
    }
  }

  /**
   * {@code FlattenIterables<T>} takes a {@code PCollection<Iterable<T>>} and returns a
   * {@code PCollection<T>} that contains all the elements from each iterable.
   * Implements {@link #iterables}.
   *
   * @param <T> the type of the elements of the input {@code Iterable}s and
   * the output {@code PCollection}
   */
  public static class FlattenIterables<T>
      extends PTransform<PCollection<? extends Iterable<T>>, PCollection<T>> {

    @Override
    public PCollection<T> apply(PCollection<? extends Iterable<T>> in) {
      Coder<? extends Iterable<T>> inCoder = in.getCoder();
      if (!(inCoder instanceof IterableLikeCoder)) {
        throw new IllegalArgumentException(
            "expecting the input Coder<Iterable> to be an IterableLikeCoder");
      }
      @SuppressWarnings("unchecked")
      Coder<T> elemCoder = ((IterableLikeCoder<T, ?>) inCoder).getElemCoder();

      return in.apply(ParDo.named("FlattenIterables").of(
          new DoFn<Iterable<T>, T>() {
            @Override
            public void processElement(ProcessContext c) {
              for (T i : c.element()) {
                c.output(i);
              }
            }
          }))
          .setCoder(elemCoder);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  static {
    DirectPipelineRunner.registerDefaultTransformEvaluator(
        FlattenPCollectionList.class,
        new DirectPipelineRunner.TransformEvaluator<FlattenPCollectionList>() {
          @Override
          public void evaluate(
              FlattenPCollectionList transform,
              DirectPipelineRunner.EvaluationContext context) {
            evaluateHelper(transform, context);
          }
        });
  }

  private static <T> void evaluateHelper(
      FlattenPCollectionList<T> transform,
      DirectPipelineRunner.EvaluationContext context) {
    List<DirectPipelineRunner.ValueWithMetadata<T>> outputElems = new ArrayList<>();
    PCollectionList<T> inputs = context.getInput(transform);

    for (PCollection<T> input : inputs.getAll()) {
      outputElems.addAll(context.getPCollectionValuesWithMetadata(input));
    }

    context.setPCollectionValuesWithMetadata(context.getOutput(transform), outputElems);
  }
}
